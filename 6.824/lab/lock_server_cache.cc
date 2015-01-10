// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


lock_server_cache::lock_server_cache()
{
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &)
{
  lock_protocol::status ret = lock_protocol::OK;
  pthread_mutex_lock(&lmap_mutex);
  lock_cache_value* lc_value = get_lock_obj(lid);
  if(lc_value->l_state == LOCKFREE){
    lc_value->l_state = LOCKED;
    ret = lock_protocol::OK;
  }
  else if(lc_value->l_state == RETRYING && lc_value->retrying_clientid == id){
    if(lc_value->waiting_clientids.size() == 0)
      lc_value->retrying_clientid.clear();
    else{
      // move front of waiting_clientid to retrying_clientid
      lc_value->retrying_clientid = lc_value->waiting_clientids.front();
      // schedule a revoke
      client_info cin = new client_info();
      cin.client_id = id;
      cin.lid = lid;
      revoke_list.push_back(cin);
      // signal releaser
      pthread_cond_signal(&releaser_cv);
    }
    lc_value->l_state = LOCKED;
    ret = lock_protocol::OK;
  }
  else{ // lock is not available
    // push to waiting_clientids
    lc_value->waiting_clientids.push_back(id);
    if(lc_value->l_state == LOCKED){
        // schedule a revoke
        client_info cin = new client_info();
        cin.client_id = id;
        cin.lid = lid;
        revoke_list.push_back(cin);
        // signal releaser
        pthread_cond_signal(&releaser_cv);
    }
    ret = lock_protocol::RETRY;;
  }
  pthread_mutex_unlock(&lmap_mutex);
  return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  pthread_mutex_lock(&lmap_mutex);
  lock_cache_value* lc_value = get_lock_obj(lid);
  lc_value->owner_clientid.clear();
  if(lc_value->waiting_clientids.size() == 0){
    lc_value->l_state = LOCKFREE;
  }
  else{
    lc_value->l_state = RETRYING;
    lc_value->waiting_clientids.push_back(id);
    if(lc_value->retrying_clientid.empty()){
      lc_value->retrying_clientid = id;
    }
    // push into RETRY list
    client_info cin = new client_info();
    cin.client_id = id;
    cin.lid = lid;
    retry_list.push_back(cin);
    // signal retryer
    pthread_cond_signal(&retry_cv);
  }
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
        pthread_cond_wait(&retry_cv, &retry_mutex);
        while (!retry_list.empty()) {
            client_info c_info = retry_list.front();
            retry_list.pop_front();
            handle h(c_info.client_id);
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
        pthread_cond_wait(&releaser_cv, &releaser_mutex);
        while (!revoke_list.empty()) {
            client_info c_info = revoke_list.front();
            revoke_list.pop_front();
            handle h(c_info.client_id);
            if (h.safebind())
                r_ret = h.safebind()->call(rlock_protocol::revoke,
                        c_info.lid, r);
            if (!h.safebind() || r_ret != rlock_protocol::OK)
                tprintf("revoke RPC failed\n");
        }
        pthread_mutex_unlock(&releaser_mutex);
    }
}

