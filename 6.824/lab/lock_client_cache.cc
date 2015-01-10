// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"


lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  rpcs *rlsrpc = new rpcs(0);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

  const char *hname;
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlsrpc->port();
  id = host.str();
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;
  lock_cache_value* lc_value = get_lock_obj(lid);
  int r;
  pthread_mutex_lock(&lc_value->client_lock_mutex);
  while(true){
    if(lc_value->lc_state == NONE){
      ret = cl->call(lock_protocol::acquire, cl->id(), lid, r);
      if(ret == lock_protocol::OK){
        lc_value->lc_state = LOCKED;
        break;
      }
      else{
        while(ret == lock_protocol::RETRY){
          pthread_cond_wait(&lc_value->client_lock_cv, &lc_value->client_lock_mutex);
          if(lc_value->lc_state == FREE){
            lc_value->lc_state = LOCKED;
            break;
          } else
            continue;
        }
      }
    }
    else if(lc_value->lc_state == FREE){
      lc_value->lc_state = LOCKED;
      break;
    }
    else{
      pthread_cond_wait(&lc_value->client_lock_cv, &lc_value->client_lock_mutex);
      if(lc_value->lc_state == FREE){
        lc_value->lc_state = LOCKED;
        break;
      } else
        continue;
    }
  }
  pthread_mutex_unlock(&lc_value->client_lock_mutex);
  return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;
  lock_cache_value* lc_value = get_lock_obj(lid);
  int r;
  pthread_mutex_lock(&lc_value->client_lock_mutex);
  pthread_cond_wait(&client_retry_cv, &client_releaser_mutex);
  if(lc_value->lc_state == LOCKED){
    lc_value->lc_state = FREE;
    pthread_cond_signal(&lc_value->client_lock_cv);
  }
  else if(lc_value->lc_state == RELEASING){
    pthread_cond_signal(&lc_value->client_revoke_cv);
  }
  pthread_mutex_unlock(&lc_value->client_lock_mutex);
  return lock_protocol::OK;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  int ret = rlock_protocol::OK;
  tprintf("got revoke from server for lid:%llu\n", lid);
  pthread_mutex_lock(&client_releaser_mutex);
  revoke_list.push_back(lid);
  pthread_cond_signal(&client_releaser_cv);
  pthread_mutex_unlock(&client_releaser_mutex);

  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  int ret = rlock_protocol::OK;
  // Push the lid to the retry list
  tprintf("got retry from server for lid:%llu\n", lid);
  pthread_mutex_lock(&client_retry_mutex);
  retry_list.push_back(lid);
  pthread_cond_signal(&client_retry_cv);
  pthread_mutex_unlock(&client_retry_mutex);

  return ret;
}

lock_client_cache::lock_cache_value*
lock_client_cache::get_lock_obj(lock_protocol::lockid_t lid)
{
    lock_cache_value *lock_cache_obj;
    pthread_mutex_lock(&client_cache_mutex);
    if (clientcacheMap.count(lid) > 0) {
        lock_cache_obj = clientcacheMap[lid];
        //printf("found!!!!\n");
    }
    else {
        lock_cache_obj = new lock_cache_value();
        VERIFY(pthread_mutex_init(&lock_cache_obj->client_lock_mutex, 0) == 0);
        VERIFY(pthread_cond_init(&lock_cache_obj->client_lock_cv, NULL) == 0);
        VERIFY(pthread_cond_init(&lock_cache_obj->client_revoke_cv, NULL) == 0);
        clientcacheMap[lid] = lock_cache_obj;
        //printf("allocating new\n");
    }
    pthread_mutex_unlock(&client_cache_mutex);
    return lock_cache_obj;
}

void
lock_client_cache::releaser(void) {
  int ret, r;
  while(true){
    pthread_mutex_lock(&client_releaser_mutex);
    pthread_cond_wait(&client_releaser_cv, &client_releaser_mutex);
    while(!revoke_list.empty()){
      lock_protocol::lockid_t lid = revoke_list.front();
      revoke_list.pop_front();
      lock_cache_value *lock_cache_obj = get_lock_obj(lid);
      pthread_mutex_lock(&lock_cache_obj->client_lock_mutex);
      tprintf("releaser: got lock checking\n");
      if (lock_cache_obj->lc_state == LOCKED) {
          lock_cache_obj->lc_state = RELEASING;
          tprintf("releaser: waiting in releasing state\n");
          pthread_cond_wait(&lock_cache_obj->client_revoke_cv,
                  &lock_cache_obj->client_lock_mutex);
      }
      tprintf("releaser: calling server release, id: %s\n", id.c_str());
      ret = cl->call(lock_protocol::release, id, lid, r);
      lock_cache_obj->lc_state = NONE;
      pthread_cond_signal(&lock_cache_obj->client_lock_cv);
      pthread_mutex_unlock(&lock_cache_obj->client_lock_mutex);
    }
    pthread_mutex_unlock(&client_releaser_mutex);
  }
}

void
lock_client_cache::retryer(void) {

}

