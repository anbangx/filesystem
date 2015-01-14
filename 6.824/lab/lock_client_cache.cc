// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"

static void *
releasethread(void *x)
{
  lock_client_cache *cc = (lock_client_cache *) x;
  cc->releaser();
  return 0;
}

static void *
retrythread(void *x)
{
  lock_client_cache *cc = (lock_client_cache *) x;
  cc->retryer();
  return 0;
}

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

  VERIFY(pthread_mutex_init(&client_cache_mutex,0) == 0);
  VERIFY(pthread_cond_init(&client_cache_cv, NULL) == 0);
  // Create retryer and releaser threads
  pthread_t retryer_thread, releaser_thread;
  VERIFY(pthread_mutex_init(&client_retry_mutex,0) == 0);
  VERIFY(pthread_cond_init(&client_retry_cv, NULL) == 0);
  VERIFY(pthread_mutex_init(&client_releaser_mutex,0) == 0);
  VERIFY(pthread_cond_init(&client_releaser_cv, NULL) == 0);

  if (pthread_create(&retryer_thread, NULL, &retrythread, (void *) this))
      tprintf("Error in creating retryer thread\n");
  if (pthread_create(&releaser_thread, NULL, &releasethread, (void *) this))
      tprintf("Error in creating releaser thread\n");
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  tprintf("lock_client_cache::acquire id:%s lid:%llu start\n", id.c_str(), lid);
  int ret = lock_protocol::OK;
  lock_cache_value* lc_value = get_lock_obj(lid);
  int r;
  pthread_mutex_lock(&lc_value->client_lock_mutex);
  while(true){
    if(lc_value->lc_state == NONE){
      lc_value->lc_state = ACQUIRING;
      ret = cl->call(lock_protocol::acquire, lid, id, r);
      if(ret == lock_protocol::OK){
        tprintf("lock_client_cache::acquire id:%s lid:%llu 1.1 get lock NONE -> LOCKED\n", id.c_str(), lid);
        lc_value->lc_state = LOCKED;
        break;
      }
      else{
        while(ret == lock_protocol::RETRY){
          tprintf("lock_client_cache::acquire id:%s lid:%llu 1.2 wait on client_lock_cv\n", id.c_str(), lid);
          pthread_cond_wait(&lc_value->client_lock_cv, &lc_value->client_lock_mutex);
          if(lc_value->lc_state == FREE){
            tprintf("lock_client_cache::acquire id:%s lid:%llu 1.3.1 get lock after waking from client_lock_cv\n", id.c_str(), lid);
            lc_value->lc_state = LOCKED;
            break;
          } else{
            tprintf("lock_client_cache::acquire id:%s lid:%llu 1.3.2 not get lock after waking from client_lock_cv\n", id.c_str(), lid);
            continue;
          }
        }
      }
    }
    else if(lc_value->lc_state == FREE){
      tprintf("lock_client_cache::acquire id:%s lid:%llu 2. get lock FREE -> LOCKED\n", id.c_str(), lid);
      lc_value->lc_state = LOCKED;
      break;
    }
    else{
      tprintf("lock_client_cache::acquire id:%s lid:%llu 3.1 lock unavailable, wait on client_lock_cv\n", id.c_str(), lid);
      pthread_cond_wait(&lc_value->client_lock_cv, &lc_value->client_lock_mutex);
      if(lc_value->lc_state == FREE){
        tprintf("lock_client_cache::acquire id:%s lid:%llu 3.2.1 get lock after waking from client_lock_cv\n", id.c_str(), lid);
        lc_value->lc_state = LOCKED;
        break;
      } else{
        tprintf("lock_client_cache::acquire id:%s lid:%llu 3.2.2 get lock after waking from client_lock_cv\n", id.c_str(), lid);
        continue;
      }
    }
  }
  pthread_mutex_unlock(&lc_value->client_lock_mutex);
  return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  tprintf("lock_client_cache::release id:%s lid:%llu start\n", id.c_str(), lid);
  int ret = lock_protocol::OK;
  lock_cache_value* lc_value = get_lock_obj(lid);
  int r;
  pthread_mutex_lock(&lc_value->client_lock_mutex);
  if(lc_value->lc_state == LOCKED){
    tprintf("lock_client_cache::release id:%s lid:%llu 1.1 lc_value->lc_state == LOCKED, release lock after waking from client_retry_cv\n", id.c_str(), lid);
    lc_value->lc_state = FREE;
    pthread_cond_signal(&lc_value->client_lock_cv);
    tprintf("lock_client_cache::release id:%s lid:%llu 1.2 signal client_retry_cv\n", id.c_str(), lid);
  }
  else if(lc_value->lc_state == RELEASING){
    pthread_cond_signal(&lc_value->client_revoke_cv);
    tprintf("lock_client_cache::release id:%s lid:%llu 2. lc_value->lc_state == RELEASING, signal client_revoke_cv\n", id.c_str(), lid);
  }
  pthread_mutex_unlock(&lc_value->client_lock_mutex);
  return lock_protocol::OK;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  int ret = rlock_protocol::OK;
  tprintf("lock_client_cache::revoke_handler id:%s got revoke from server for lid:%llu\n", id.c_str(), lid);
  pthread_mutex_lock(&client_releaser_mutex);
  revoke_list.push_back(lid);
  pthread_cond_signal(&client_releaser_cv);
  tprintf("lock_client_cache::revoke_handler id:%s add lid:%llu to revoke_list and signal client_releaser_cv\n", id.c_str(), lid);
  pthread_mutex_unlock(&client_releaser_mutex);

  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  int ret = rlock_protocol::OK;
  // Push the lid to the retry list
  tprintf("lock_client_cache::retry_handler id:%s got retry from server for lid:%llu\n", id.c_str(), lid);
  pthread_mutex_lock(&client_retry_mutex);
  retry_list.push_back(lid);
  pthread_cond_signal(&client_retry_cv);
  tprintf("lock_client_cache::revoke_handler id:%s add lid:%llu to retry_list and signal client_retry_cv\n", id.c_str(), lid);
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
        tprintf("lock_client_cache::get_lock_obj found!!!!\n");
    }
    else {
        lock_cache_obj = new lock_cache_value();
        VERIFY(pthread_mutex_init(&lock_cache_obj->client_lock_mutex, 0) == 0);
        VERIFY(pthread_cond_init(&lock_cache_obj->client_lock_cv, NULL) == 0);
        VERIFY(pthread_cond_init(&lock_cache_obj->client_revoke_cv, NULL) == 0);
        clientcacheMap[lid] = lock_cache_obj;
        tprintf("lock_client_cache::get_lock_obj allocating new\n");
    }
    pthread_mutex_unlock(&client_cache_mutex);
    return lock_cache_obj;
}

void
lock_client_cache::releaser(void) {
  tprintf("lock_client_cache::releaser start\n");
  int ret, r;
  while(true){
    pthread_mutex_lock(&client_releaser_mutex);
    pthread_cond_wait(&client_releaser_cv, &client_releaser_mutex);
    while(!revoke_list.empty()){
      lock_protocol::lockid_t lid = revoke_list.front();
      revoke_list.pop_front();
      lock_cache_value *lock_cache_obj = get_lock_obj(lid);
      pthread_mutex_lock(&lock_cache_obj->client_lock_mutex);
      tprintf("lock_client_cache::releaser id:%s 1. got lock checking\n", id.c_str());
      if (lock_cache_obj->lc_state == LOCKED) {
          lock_cache_obj->lc_state = RELEASING;
          tprintf("lock_client_cache::releaser id:%s 1.1 lid:%llu LOCKED -> RELEASING and wait on client_revoke_cv\n", id.c_str(), lid);
          pthread_cond_wait(&lock_cache_obj->client_revoke_cv,
                  &lock_cache_obj->client_lock_mutex);
      }
      tprintf("lock_client_cache::releaser id:%s 2. calling server release\n", id.c_str());
      ret = cl->call(lock_protocol::release, lid, id, r);
      lock_cache_obj->lc_state = NONE;
      pthread_cond_signal(&lock_cache_obj->client_lock_cv);
      tprintf("lock_client_cache::releaser id:%s 3. lid:%llu signal client_lock_cv\n", id.c_str(), lid);
      pthread_mutex_unlock(&lock_cache_obj->client_lock_mutex);
    }
    pthread_mutex_unlock(&client_releaser_mutex);
  }
}

void
lock_client_cache::retryer(void) {
  tprintf("lock_client_cache::retryer start\n");
  int r;
  int ret;
  while(true) {
      pthread_mutex_lock(&client_retry_mutex);
      pthread_cond_wait(&client_retry_cv, &client_retry_mutex);
      while (!retry_list.empty()) {
          lock_protocol::lockid_t lid = retry_list.front();
          retry_list.pop_front();
          lock_cache_value *lock_cache_obj = get_lock_obj(lid);
          tprintf("lock_client_cache::retryer id:%s for lid:%llu\n", id.c_str(), lid);
          pthread_mutex_lock(&lock_cache_obj->client_lock_mutex);
          ret = cl->call(lock_protocol::acquire, lid, id, r);
          if (ret == lock_protocol::OK) {
              lock_cache_obj->lc_state = FREE;
              tprintf("lock_client_cache::retryer id:%s lid:%llu OK -> FREE and signal client_lock_cv\n", id.c_str(), lid);
              pthread_cond_signal(&lock_cache_obj->client_lock_cv);
              pthread_mutex_unlock(&lock_cache_obj->client_lock_mutex);
          }
          else
              tprintf("lock_client_cache::retryer id:%s fail, should never happen, ret:%d\n", id.c_str(), ret);
      }
      pthread_mutex_unlock(&client_retry_mutex);
  }
}

