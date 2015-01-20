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

#ifndef PURGE_QUEUE_H
#define PURGE_QUEUE_H

#include "include/elist.h"
#include <list>

class MDS;
class PerfCounters;
class CInode;
class CDentry;

#undef HANDLE_ROGUE_REFS

class PurgeQueue
{
  protected:
  // Has passed through eval_stray and still has refs
  elist<CDentry*> delayed_eval_stray;

  // No more refs, can purge these
  std::list<CDentry*> ready_for_purge;

  // Global references for doing I/O
  MDS *mds;
  /** So, it sucks to have a circular reference here
   * back to our parent mdcache.  It's used for remove_inode
   * and for touch_dentry_bottom.  Both of those are
   * actually pure *outputs* from this entity: we
   * dont need a ref to mdcache, we just need a mailbox to
   * send notifications to that we want to remove or
   * bump this inode.  Hmm.  */
  MDCache *mdcache;
  PerfCounters *logger;

  // Throttled allowances
  uint64_t ops_in_flight;
  uint64_t files_purging;

  // Statistics
  uint64_t num_strays;
  uint64_t num_strays_purging;
  uint64_t num_strays_delayed;

  void purge(CDentry *dn, uint32_t op_allowance);
  void _purge_stray_purged(CDentry *dn, uint32_t ops, int r=0);
  void _purge_stray_logged(CDentry *dn, version_t pdv, LogSegment *ls);
#ifdef HANDLE_ROGUE_REFS
  void _purge_stray_logged_truncate(CDentry *dn, LogSegment *ls);
  friend class C_PurgeStrayLoggedTruncate;
#endif


  friend class PurgeQueueIOContext;
  friend class PurgeQueueContext;

  friend class C_PurgeStrayLogged;
  friend class C_IO_PurgeStrayPurged;

  void _advance();
  bool _consume(CDentry *dn);
  uint32_t _calculate_ops_required(CInode *in);

  public:

  void enqueue(CDentry *dn);
  void advance_delayed();
  void eval_stray(CDentry *dn, bool delay=false);
  void reintegrate_stray(CDentry *dn, CDentry *rlink);
  void migrate_stray(CDentry *dn, mds_rank_t dest);

  PurgeQueue(MDS *mds, MDCache *mdc);
  void set_logger(PerfCounters *l) {logger = l;}
  void notify_stray_created();
};

#endif  // PURGE_QUEUE_H
