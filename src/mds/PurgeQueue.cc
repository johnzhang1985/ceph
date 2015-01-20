// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#include "common/perf_counters.h"

#include "osdc/Objecter.h"
#include "osdc/Filer.h"
#include "mds/MDS.h"
#include "mds/MDCache.h"
#include "mds/MDLog.h"
#include "mds/CDir.h"
#include "mds/CDentry.h"
#include "events/EUpdate.h"
#include "messages/MClientRequest.h"

#include "PurgeQueue.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix _prefix(_dout, mds)
static ostream& _prefix(std::ostream *_dout, MDS *mds) {
  return *_dout << "mds." << mds->get_nodeid() << ".cache.pq ";
}

class PurgeQueueIOContext : public virtual MDSIOContextBase {
protected:
  PurgeQueue *pq;
  virtual MDS *get_mds()
  {
    return pq->mds;
  }
public:
  PurgeQueueIOContext(PurgeQueue *pq_) : pq(pq_) {}
};


class PurgeQueueContext : public virtual MDSInternalContextBase {
protected:
  PurgeQueue *pq;
  virtual MDS *get_mds()
  {
    return pq->mds;
  }
public:
  PurgeQueueContext(PurgeQueue *pq_) : pq(pq_) {}
};


class C_IO_PurgeStrayPurged : public PurgeQueueIOContext {
  CDentry *dn;
  // How many ops_in_flight were allocated to this purge?
  uint32_t ops_allowance;
public:
  C_IO_PurgeStrayPurged(PurgeQueue *pq_, CDentry *d, uint32_t ops) : 
    PurgeQueueIOContext(pq_), dn(d), ops_allowance(ops) { }
  void finish(int r) {
    assert(r == 0 || r == -ENOENT);
    pq->_purge_stray_purged(dn, ops_allowance, r);
  }
};

/**
 * Purge a dentry from a stray directory.  This function
 * is called once eval_stray is satisfied and PurgeQueue
 * throttling is also satisfied.  There is no going back
 * at this stage!
 */
void PurgeQueue::purge(CDentry *dn, uint32_t op_allowance)
{
  CDentry::linkage_t *dnl = dn->get_projected_linkage();
  CInode *in = dnl->get_inode();
  dout(10) << __func__ << " " << *dn << " " << *in << dendl;
  assert(!dn->is_replicated());

  dn->state_set(CDentry::STATE_PURGING);
  dn->get(CDentry::PIN_PURGING);
  in->state_set(CInode::STATE_PURGING);

  num_strays_purging++;
  logger->set(l_mdc_num_strays_purging, num_strays_purging);

  if (dn->item_stray.is_on_list()) {
    dn->item_stray.remove_myself();
    num_strays_delayed--;
    logger->set(l_mdc_num_strays_delayed, num_strays_delayed);
  }

  if (in->is_dirty_parent())
    in->clear_dirty_parent();

  // CHEAT.  there's no real need to journal our intent to purge, since
  // that is implicit in the dentry's presence and non-use in the stray
  // dir.  on recovery, we'll need to re-eval all strays anyway.
  
  SnapContext nullsnapc;
  C_GatherBuilder gather(
    g_ceph_context,
    new C_OnFinisher(new C_IO_PurgeStrayPurged(
        this, dn, op_allowance), &mds->finisher));

  if (in->is_dir()) {
    object_locator_t oloc(mds->mdsmap->get_metadata_pool());
    std::list<frag_t> ls;
    if (!in->dirfragtree.is_leaf(frag_t()))
      in->dirfragtree.get_leaves(ls);
    ls.push_back(frag_t());
    for (std::list<frag_t>::iterator p = ls.begin();
         p != ls.end();
         ++p) {
      object_t oid = CInode::get_object_name(in->inode.ino, *p, "");
      dout(10) << __func__ << " remove dirfrag " << oid << dendl;
      mds->objecter->remove(oid, oloc, nullsnapc, ceph_clock_now(g_ceph_context),
                            0, NULL, gather.new_sub());
    }
    assert(gather.has_subs());
    gather.activate();
    return;
  }

  const SnapContext *snapc;
  SnapRealm *realm = in->find_snaprealm();
  if (realm) {
    dout(10) << " realm " << *realm << dendl;
    snapc = &realm->get_snap_context();
  } else {
    dout(10) << " NO realm, using null context" << dendl;
    snapc = &nullsnapc;
    assert(in->last == CEPH_NOSNAP);
  }

  if (in->is_file()) {
    uint64_t period = (uint64_t)in->inode.layout.fl_object_size *
		      (uint64_t)in->inode.layout.fl_stripe_count;
    uint64_t to = in->inode.get_max_size();
    to = MAX(in->inode.size, to);
    // when truncating a file, the filer does not delete stripe objects that are
    // truncated to zero. so we need to purge stripe objects up to the max size
    // the file has ever been.
    to = MAX(in->inode.max_size_ever, to);
    if (to && period) {
      uint64_t num = (to + period - 1) / period;
      dout(10) << __func__ << " 0~" << to << " objects 0~" << num
	       << " snapc " << snapc << " on " << *in << dendl;
      mds->filer->purge_range(in->inode.ino, &in->inode.layout, *snapc,
			      0, num, ceph_clock_now(g_ceph_context), 0,
			      gather.new_sub());
    }
  }

  inode_t *pi = in->get_projected_inode();
  object_t oid = CInode::get_object_name(pi->ino, frag_t(), "");
  // remove the backtrace object if it was not purged
  if (!gather.has_subs()) {
    object_locator_t oloc(pi->layout.fl_pg_pool);
    dout(10) << __func__ << " remove backtrace object " << oid
	     << " pool " << oloc.pool << " snapc " << snapc << dendl;
    mds->objecter->remove(oid, oloc, *snapc, ceph_clock_now(g_ceph_context), 0,
			  NULL, gather.new_sub());
  }
  // remove old backtrace objects
  for (vector<int64_t>::iterator p = pi->old_pools.begin();
       p != pi->old_pools.end();
       ++p) {
    object_locator_t oloc(*p);
    dout(10) << __func__ << " remove backtrace object " << oid
	     << " old pool " << *p << " snapc " << snapc << dendl;
    mds->objecter->remove(oid, oloc, *snapc, ceph_clock_now(g_ceph_context), 0,
			  NULL, gather.new_sub());
  }
  assert(gather.has_subs());
  gather.activate();
}

class C_PurgeStrayLogged : public PurgeQueueContext {
  CDentry *dn;
  version_t pdv;
  LogSegment *ls;
public:
  C_PurgeStrayLogged(PurgeQueue *pq_, CDentry *d, version_t v, LogSegment *s) : 
    PurgeQueueContext(pq_), dn(d), pdv(v), ls(s) { }
  void finish(int r) {
    pq->_purge_stray_logged(dn, pdv, ls);
  }
};

#ifdef HANDLE_ROGUE_REFS
class C_PurgeStrayLoggedTruncate : public PurgeQueueContext {
  CDentry *dn;
  LogSegment *ls;
public:
  C_PurgeStrayLoggedTruncate(PurgeQueue *pq_, CDentry *d, LogSegment *s) : 
    PurgeQueueContext(pq_), dn(d), ls(s) { }
  void finish(int r) {
    pq->_purge_stray_logged_truncate(dn, ls);
  }
};
#endif

void PurgeQueue::_purge_stray_purged(CDentry *dn, uint32_t ops_allowance, int r)
{
  assert (r == 0 || r == -ENOENT);
  CInode *in = dn->get_projected_linkage()->get_inode();
  dout(10) << "_purge_stray_purged " << *dn << " " << *in << dendl;

  if (in->get_num_ref() == (int)in->is_dirty() &&
      dn->get_num_ref() == (int)dn->is_dirty() + !!in->get_num_ref() + 1/*PIN_PURGING*/) {
    // kill dentry.
    version_t pdv = dn->pre_dirty();
    dn->push_projected_linkage(); // NULL

    EUpdate *le = new EUpdate(mds->mdlog, "purge_stray");
    mds->mdlog->start_entry(le);

    // update dirfrag fragstat, rstat
    CDir *dir = dn->get_dir();
    fnode_t *pf = dir->project_fnode();
    pf->version = dir->pre_dirty();
    if (in->is_dir())
      pf->fragstat.nsubdirs--;
    else
      pf->fragstat.nfiles--;
    pf->rstat.sub(in->inode.accounted_rstat);

    le->metablob.add_dir_context(dn->dir);
    EMetaBlob::dirlump& dl = le->metablob.add_dir(dn->dir, true);
    le->metablob.add_null_dentry(dl, dn, true);
    le->metablob.add_destroyed_inode(in->ino());

    mds->mdlog->submit_entry(le, new C_PurgeStrayLogged(this, dn, pdv, mds->mdlog->get_current_segment()));

    num_strays_purging--;
    num_strays--;
    logger->set(l_mdc_num_strays, num_strays);
    logger->set(l_mdc_num_strays_purging, num_strays_purging);
    logger->inc(l_mdc_strays_purged);
  } else {
#ifdef HANDLE_ROGUE_REFS
    // new refs.. just truncate to 0
    EUpdate *le = new EUpdate(mds->mdlog, "purge_stray truncate");
    mds->mdlog->start_entry(le);
    
    inode_t *pi = in->project_inode();
    pi->size = 0;
    pi->client_ranges.clear();
    pi->truncate_size = 0;
    pi->truncate_from = 0;
    pi->version = in->pre_dirty();

    le->metablob.add_dir_context(dn->dir);
    le->metablob.add_primary_dentry(dn, in, true);

    mds->mdlog->submit_entry(le, new C_PurgeStrayLoggedTruncate(this, dn, mds->mdlog->get_current_segment()));
#else
    /*
     * Nobody should be taking new references to an inode when it
     * is being purged.  However, there may be some buggy code that
     * does so.
     */

    derr << "Rogue reference after purge to " << *dn << dendl;
    assert(0 == "rogue reference to purging inode");
#endif
  }

  // Release resources
  dout(10) << __func__ << ": decrementing allowance "
    << ops_allowance << " from " << ops_in_flight << " in flight" << dendl;
  assert(ops_in_flight >= ops_allowance);
  ops_in_flight -= ops_allowance;
  files_purging -= 1;
  _advance();
}

void PurgeQueue::_purge_stray_logged(CDentry *dn, version_t pdv, LogSegment *ls)
{
  CInode *in = dn->get_linkage()->get_inode();
  dout(10) << "_purge_stray_logged " << *dn << " " << *in << dendl;

  assert(!in->state_test(CInode::STATE_RECOVERING));

  // unlink
  assert(dn->get_projected_linkage()->is_null());
  dn->dir->unlink_inode(dn);
  dn->pop_projected_linkage();
  dn->mark_dirty(pdv, ls);

  dn->dir->pop_and_dirty_projected_fnode(ls);

  in->state_clear(CInode::STATE_ORPHAN);
  dn->state_clear(CDentry::STATE_PURGING);
  dn->put(CDentry::PIN_PURGING);

  // drop inode
  if (in->is_dirty())
    in->mark_clean();

  mdcache->remove_inode(in);

  // drop dentry?
  if (dn->is_new()) {
    dout(20) << " dn is new, removing" << dendl;
    dn->mark_clean();
    dn->dir->remove_dentry(dn);
  } else {
    mdcache->touch_dentry_bottom(dn);  // drop dn as quickly as possible.
  }
}

#ifdef HANDLE_ROGUE_REFS
void PurgeQueue::_purge_stray_logged_truncate(CDentry *dn, LogSegment *ls)
{
  CInode *in = dn->get_projected_linkage()->get_inode();
  dout(10) << "_purge_stray_logged_truncate " << *dn << " " << *in << dendl;

  dn->state_clear(CDentry::STATE_PURGING);
  dn->put(CDentry::PIN_PURGING);

  in->pop_and_dirty_projected_inode(ls);

  eval_stray(dn);
}
#endif

void PurgeQueue::enqueue(CDentry *dn)
{
  // Try to purge immediately if possible, else enqueue
  bool consumed = _consume(dn);
  if (consumed) {
    dout(10) << __func__ << ": purging this dentry immediately: "
      << *dn << dendl;
  } else {
    dout(10) << __func__ << ": enqueuing tihs dentry for later purge: "
      << *dn << dendl;
    ready_for_purge.push_back(dn);
  }
}

void PurgeQueue::_advance()
{
  std::list<CDentry*>::iterator i;
  for (i = ready_for_purge.begin();
       i != ready_for_purge.end(); ++i) {
    const bool consumed = _consume(*i);
    if (!consumed) {
      break;
    }
  }

  // Erase all the ones that returned true from _consume
  ready_for_purge.erase(ready_for_purge.begin(), i);
}

/**
 * Attempt to purge an inode, if throttling permits
 * it.  Note that there are compromises to how
 * the throttling works, in interests of simplicity:
 *  * If insufficient ops are available to execute
 *    the next item on the queue, we block even if
 *    there are items further down the queue requiring
 *    fewer ops which might be executable
 *  * The ops considered "in use" by a purge will be
 *    an overestimate over the period of execution, as
 *    we count filer_max_purge_ops and ops for old backtraces
 *    as in use throughout, even though towards the end
 *    of the purge the actual ops in flight will be
 *    lower.
 *  * The ops limit may be exceeded if the number of ops
 *    required by a single inode is greater than the
 *    limit, for example directories with very many
 *    fragments.
 *
 * Return true if we successfully consumed resource,
 * false if insufficient resource was available.
 */
bool PurgeQueue::_consume(CDentry *dn)
{
  const int files_avail = g_conf->mds_max_purge_files - files_purging;

  if (files_avail <= 0) {
    dout(20) << __func__ << ": throttling on max files" << dendl;
    return false;
  }

  CDentry::linkage_t *dnl = dn->get_projected_linkage();
  assert(dnl->is_primary());
  CInode *in = dnl->get_inode();

  // Calculate how much of the ops allowance is available, allowing
  // for the case where the limit is currently being exceeded.
  uint32_t ops_avail;
  if (ops_in_flight <= g_conf->mds_max_purge_ops) {
    ops_avail = g_conf->mds_max_purge_ops - ops_in_flight;
  } else {
    ops_avail = 0;
  }

  const uint32_t ops_required = _calculate_ops_required(in);
  /* The ops_in_flight > 0 condition here handles the case where the
   * ops required by this inode would never fit in the limit: we wait
   * instead until nothing else is running */
  if (ops_in_flight > 0 && ops_avail < ops_required) {
    dout(20) << __func__ << ": throttling on max ops (require "
             << ops_required << dendl;
    return false;
  }

  // Resources are available, acquire them and execute the purge
  files_purging += 1;
  dout(10) << __func__ << ": allocating allowance "
    << ops_required << " to " << ops_in_flight << " in flight" << dendl;
  ops_in_flight += ops_required;
  purge(dn, ops_required);
  return true;
}


/**
 * Return the maximum number of concurrent RADOS ops that
 * may be executed while purging this inode.
 */
uint32_t PurgeQueue::_calculate_ops_required(CInode *in)
{
  uint32_t ops_required = 0;
  if (in->is_dir()) {
    // Directory, count dirfrags to be deleted
    std::list<frag_t> ls;
    if (!in->dirfragtree.is_leaf(frag_t())) {
      in->dirfragtree.get_leaves(ls);
    }
    // One for the root, plus any leaves
    ops_required = 1 + ls.size();
  } else {
    // File, work out concurrent Filer::purge deletes
    const uint64_t period = (uint64_t)in->inode.layout.fl_object_size *
		      (uint64_t)in->inode.layout.fl_stripe_count;
    const uint64_t to = MAX(in->inode.max_size_ever,
            MAX(in->inode.size, in->inode.get_max_size()));

    const uint64_t num = MAX(1, (to + period - 1) / period);
    ops_required = MIN(num, g_conf->filer_max_purge_ops);

    // Account for deletions for old pools
    ops_required += in->get_projected_inode()->old_pools.size();
  }

  return ops_required;
}

void PurgeQueue::advance_delayed()
{
  for (elist<CDentry*>::iterator p = delayed_eval_stray.begin(); !p.end(); ) {
    CDentry *dn = *p;
    ++p;
    dn->item_stray.remove_myself();
    num_strays_delayed--;
    eval_stray(dn);
  }
  logger->set(l_mdc_num_strays_delayed, num_strays_delayed);
}

/**
 * FIXME this is a bit artifical (for stats purposes) because
 * we own the num_strays counter, so MDCache has to tell us
 * when get_or_create_stray_dentry ends up creating a dentry.
 * Can we manage that for it somehow?
 *
 * FIXME make sure that updates to num_strays are also done on export/import
 * in multi-mds scenario!
 */
void PurgeQueue::notify_stray_created()
{
  num_strays++;
  logger->set(l_mdc_num_strays, num_strays);
  logger->inc(l_mdc_strays_created);
}


struct C_EvalStray : public PurgeQueueContext {
  CDentry *dn;
  C_EvalStray(PurgeQueue *pq, CDentry *d) : PurgeQueueContext(pq), dn(d) {}
  void finish(int r) {
    pq->eval_stray(dn);
  }
};


void PurgeQueue::eval_stray(CDentry *dn, bool delay)
{
  dout(10) << "eval_stray " << *dn << dendl;
  CDentry::linkage_t *dnl = dn->get_projected_linkage();
  dout(10) << " inode is " << *dnl->get_inode() << dendl;
  assert(dnl->is_primary());
  CInode *in = dnl->get_inode();
  assert(in);

  assert(dn->get_dir()->get_inode()->is_stray());

  if (!dn->is_auth()) {
    // has to be mine
    // move to bottom of lru so that we trim quickly!

    mdcache->touch_dentry_bottom(dn);
    return;
  }

  // purge?
  if (in->inode.nlink == 0) {
    if (in->is_dir()) {
      // past snaprealm parents imply snapped dentry remote links.
      // only important for directories.  normal file data snaps are handled
      // by the object store.
      if (in->snaprealm && in->snaprealm->has_past_parents()) {
	if (!in->snaprealm->have_past_parents_open() &&
	    !in->snaprealm->open_parents(new C_EvalStray(this, dn)))
	  return;
	in->snaprealm->prune_past_parents();
	if (in->snaprealm->has_past_parents()) {
	  dout(20) << "  has past parents " << in->snaprealm->srnode.past_parents << dendl;
	  return;  // not until some snaps are deleted.
	}
      }
    }
    if (dn->is_replicated()) {
      dout(20) << " replicated" << dendl;
      return;
    }
    if (dn->is_any_leases() || in->is_any_caps()) {
      dout(20) << " caps | leases" << dendl;
      return;  // wait
    }
    if (dn->state_test(CDentry::STATE_PURGING)) {
      dout(20) << " already purging" << dendl;
      return;  // already purging
    }
    if (in->state_test(CInode::STATE_NEEDSRECOVER) ||
	in->state_test(CInode::STATE_RECOVERING)) {
      dout(20) << " pending recovery" << dendl;
      return;  // don't mess with file size probing
    }
    if (in->get_num_ref() > (int)in->is_dirty() + (int)in->is_dirty_parent()) {
      dout(20) << " too many inode refs" << dendl;
      return;
    }
    if (dn->get_num_ref() > (int)dn->is_dirty() + !!in->get_num_ref()) {
      dout(20) << " too many dn refs" << dendl;
      return;
    }
    if (delay) {
      if (!dn->item_stray.is_on_list()) {
	delayed_eval_stray.push_back(&dn->item_stray);
	num_strays_delayed++;
	logger->set(l_mdc_num_strays_delayed, num_strays_delayed);
      }
    } else {
      if (in->is_dir())
	in->close_dirfrags();
      enqueue(dn);
    }
  }
  else if (in->inode.nlink >= 1) {
    // trivial reintegrate?
    if (!in->remote_parents.empty()) {
      CDentry *rlink = *in->remote_parents.begin();
      
      // don't do anything if the remote parent is projected, or we may
      // break user-visible semantics!
      // NOTE: we repeat this check in _rename(), since our submission path is racey.
      if (!rlink->is_projected()) {
	if (rlink->is_auth() && rlink->dir->can_auth_pin())
	  reintegrate_stray(dn, rlink);
	
	if (!rlink->is_auth() && dn->is_auth())
	  migrate_stray(dn, rlink->authority().first);
      }
    }
  } else {
    // wait for next use.
  }
}


void PurgeQueue::reintegrate_stray(CDentry *straydn, CDentry *rdn)
{
  dout(10) << __func__ << " " << *straydn << " into " << *rdn << dendl;
  
  // rename it to another mds.
  filepath src;
  straydn->make_path(src);
  filepath dst;
  rdn->make_path(dst);

  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_RENAME);
  req->set_filepath(dst);
  req->set_filepath2(src);
  req->set_tid(mds->issue_tid());

  mds->send_message_mds(req, rdn->authority().first);
}
 

void PurgeQueue::migrate_stray(CDentry *dn, mds_rank_t to)
{
  CInode *in = dn->get_linkage()->get_inode();
  assert(in);
  CInode *diri = dn->dir->get_inode();
  assert(diri->is_stray());
  dout(10) << "migrate_stray from mds." << MDS_INO_STRAY_OWNER(diri->inode.ino)
	   << " to mds." << to
	   << " " << *dn << " " << *in << dendl;

  // rename it to another mds.
  filepath src;
  dn->make_path(src);

  string dname;
  in->name_stray_dentry(dname);
  filepath dst(dname, MDS_INO_STRAY(to, 0));

  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_RENAME);
  req->set_filepath(dst);
  req->set_filepath2(src);
  req->set_tid(mds->issue_tid());

  mds->send_message_mds(req, to);
}

  PurgeQueue::PurgeQueue(MDS *mds, MDCache *mdc)
  : delayed_eval_stray(member_offset(CDentry, item_stray)),
    mds(mds), mdcache(mdc), logger(NULL),
    ops_in_flight(0), files_purging(0),
    num_strays(0), num_strays_purging(0), num_strays_delayed(0)
{
  assert(mds != NULL);

  assert(g_conf->mds_max_purge_files > 0);
  assert(g_conf->mds_max_purge_ops >= g_conf->filer_max_purge_ops);
}


