// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

static void *
releasethread(void *x)
{
    lock_server_cache *cc = (lock_server_cache *) x;
    cc->releaser();
    return 0;
}

static void *
retryerthread(void *x)
{
    lock_server_cache *cc = (lock_server_cache *) x;
    cc->retryer();
    return 0;
}

lock_server_cache::lock_server_cache()
{
  pthread_mutex_init(&lmap_mutex, NULL);
  pthread_cond_init(&lmap_state_cv, NULL);
  pthread_t retryer_thread, releaser_thread;
  VERIFY(pthread_mutex_init(&retry_mutex, 0) == 0);
  VERIFY(pthread_cond_init(&retry_cv, NULL) == 0);
  VERIFY(pthread_mutex_init(&releaser_mutex, 0) == 0);
  VERIFY(pthread_cond_init(&releaser_cv, NULL) == 0);

  if (pthread_create(&retryer_thread, NULL, &retryerthread, (void *) this))
      tprintf("Error in creating retryer thread\n");
  if (pthread_create(&releaser_thread, NULL, &releasethread, (void *)this))
      tprintf("Error in creating releaser thread\n");
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &)
{
  tprintf("lock_server_cache::acquire lid:%llu start\n", lid);
  lock_protocol::status ret = lock_protocol::OK;
  pthread_mutex_lock(&lmap_mutex);
  lock_cache_value* lc_value = get_lock_obj(lid);
  if(lc_value->l_state == LOCKFREE){
    tprintf("lock_server_cache::acquire lid:%llu 1.1 id:%s LOCKFREE -> LOCKED\n", lid, id.c_str());
    lc_value->l_state = LOCKED;
    lc_value->owner_clientid = id;
    ret = lock_protocol::OK;
  }
  else if(lc_value->l_state == RETRYING && lc_value->retrying_clientid == id){
    tprintf("lock_server_cache::acquire lid:%llu 1.2 id:%s RETRYING -> LOCKED\n", lid, id.c_str());
    lc_value->waiting_clientids.pop_front();
    lc_value->l_state = LOCKED;
    lc_value->owner_clientid = id;
    if(lc_value->waiting_clientids.size() > 0){
      lc_value->l_state = REVOKING;
      pthread_mutex_lock(&releaser_mutex);
      client_info c_info;
      c_info.client_id = id;
      c_info.lid = lid;
      revoke_list.push_back(c_info);
      pthread_cond_signal(&releaser_cv);
      pthread_mutex_unlock(&releaser_mutex);
    }
    ret = lock_protocol::OK;
  }
  else{ // lock is not available
    // push to waiting_clientids
    tprintf("lock_server_cache::acquire lid:%llu 1.3.1 add %s to waiting client\n", lid, id.c_str());
    lc_value->waiting_clientids.push_back(id);
    if(lc_value->l_state == LOCKED){
      lc_value->l_state = REVOKING;
      pthread_mutex_lock(&releaser_mutex);
      client_info c_info;
      c_info.client_id = lc_value->owner_clientid;
      c_info.lid = lid;
      revoke_list.push_back(c_info);
      pthread_cond_signal(&releaser_cv);
      pthread_mutex_unlock(&releaser_mutex);
      tprintf("lock_server_cache::acquire lid:%llu is LOCKED, schedule a revoke on %s and signal releaser_cv\n", lid, lc_value->owner_clientid.c_str());
    }
    ret = lock_protocol::RETRY;
  }
  pthread_mutex_unlock(&lmap_mutex);
  return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{
  tprintf("lock_server_cache::release lid:%llu id:%s start\n", lid, id.c_str());
  lock_protocol::status ret = lock_protocol::OK;
  pthread_mutex_lock(&lmap_mutex);
  lock_cache_value* lc_value = get_lock_obj(lid);
  lc_value->owner_clientid.clear();
  if(lc_value->waiting_clientids.size() == 0){
    tprintf("lock_server_cache::release 1.1 lid:%llu id:%s  empty in waiting list, -> LOCKFREE\n", lid, id.c_str());
    lc_value->l_state = LOCKFREE;
  }
  else{
    tprintf("lock_server_cache::release 1.2.1 lid:%llu id:%s change lock state to RETRYING\n", lid, id.c_str());
    lc_value->l_state = RETRYING;
    client_info c_info;
    c_info.client_id = lc_value->waiting_clientids.front();
    c_info.lid = lid;
    lc_value->retrying_clientid = c_info.client_id;
    pthread_mutex_lock(&retry_mutex);
    retry_list.push_back(c_info);
    tprintf("lock_server_cache::release 1.2.2 lid:%llu id:%s move client_id %s from waiting list and add to retry_list\n", lid, id.c_str(), c_info.client_id.c_str());
    pthread_cond_signal(&retry_cv);
    tprintf("lock_server_cache::release 1.2.3 lid:%llu id:%s signal retry_cv\n", lid, id.c_str());
    pthread_mutex_unlock(&retry_mutex);
  }
  pthread_mutex_unlock(&lmap_mutex);
  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

lock_server_cache::lock_cache_value* lock_server_cache::get_lock_obj(lock_protocol::lockid_t lid)
{
    lock_cache_value *lock_cache_obj;
    if (tLockMap.count(lid) > 0)
        lock_cache_obj = tLockMap[lid];
    else {
        lock_cache_obj = new lock_cache_value();
        tLockMap[lid] = lock_cache_obj;
    }
    return lock_cache_obj;
}

void
lock_server_cache::retryer(void) {
    int r;
    rlock_protocol::status r_ret;
    while(true) {
        pthread_mutex_lock(&retry_mutex);
        tprintf("lock_server_cache::retryer wait on retry_cv\n");
        pthread_cond_wait(&retry_cv, &retry_mutex);
        tprintf("lock_server_cache::retryer wake from retry_cv\n");
        while (!retry_list.empty()) {
            client_info c_info = retry_list.front();
            retry_list.pop_front();
            handle h(c_info.client_id);
            tprintf("lock_server_cache::retryer remove %s from retry_list and call rlock_protocol::retry\n", c_info.client_id.c_str());
            if (h.safebind())
                r_ret = h.safebind()->call(rlock_protocol::retry,
                        c_info.lid, r);
            if (!h.safebind() || r_ret != rlock_protocol::OK)
                tprintf("retry RPC failed\n");
        }
        pthread_mutex_unlock(&retry_mutex);
    }
}

void
lock_server_cache::releaser(void) {
    int r;
    rlock_protocol::status r_ret;
    while(true) {
        pthread_mutex_lock(&releaser_mutex);
        tprintf("lock_server_cache::releaser wait on releaser_cv\n");
        pthread_cond_wait(&releaser_cv, &releaser_mutex);
        tprintf("lock_server_cache::releaser wake from releaser_cv\n");
        while (!revoke_list.empty()) {
            client_info c_info = revoke_list.front();
            revoke_list.pop_front();
            handle h(c_info.client_id);
            tprintf("lock_server_cache::releaser remove %s from revoke_list and call rlock_protocol::revoke\n", c_info.client_id.c_str());
            if (h.safebind())
                r_ret = h.safebind()->call(rlock_protocol::revoke,
                        c_info.lid, r);
            if (!h.safebind() || r_ret != rlock_protocol::OK)
                tprintf("revoke RPC failed\n");
        }
        pthread_mutex_unlock(&releaser_mutex);
    }
}

