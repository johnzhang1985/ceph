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

#ifndef STRAY_MANAGER_H
#define STRAY_MANAGER_H

#include "include/elist.h"
#include <list>

class MDS;
class PerfCounters;
class CInode;
class CDentry;

class StrayManager
{
  protected:
  // Has passed through eval_stray and still has refs
  elist<CDentry*> delayed_eval_stray;

  // No more refs, can purge these
  std::list<CDentry*> ready_for_purge;

  // Global references for doing I/O
  MDS *mds;
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

  friend class StrayManagerIOContext;
  friend class StrayManagerContext;

  friend class C_PurgeStrayLogged;
  friend class C_IO_PurgeStrayPurged;

  void _advance();
  bool _consume(CDentry *dn);
  uint32_t _calculate_ops_required(CInode *in);

  void reintegrate_stray(CDentry *dn, CDentry *rlink);

  // My public interface is for consumption by MDCache
  public:

  void enqueue(CDentry *dn);
  void advance_delayed();
  bool eval_stray(CDentry *dn, bool delay=false);
  void eval_remote_stray(CDentry *stray_dn, CDentry *remote_dn=NULL);
  void migrate_stray(CDentry *dn, mds_rank_t dest);

  StrayManager(MDS *mds);
  void set_logger(PerfCounters *l) {logger = l;}
  void notify_stray_created();
  void notify_stray_removed();
  void abort_queue();
};

#endif  // STRAY_MANAGER_H
