// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>


lock_server::lock_server():
  nacquire (0)
{
  pthread_mutex_init(&mutex, NULL);
}

lock_server::~lock_server(){
  pthread_mutex_destroy(&mutex);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  printf("server acquire\n");
  lock_protocol::status ret;
  printf("acquire request from clt %d\n", clt);

  pthread_mutex_lock(&mutex);
  if(lock_map.find(lid) != lock_map.end() && lock_map.find(lid)->second == lock_server::locked){
    printf("RETRY\n");
    ret = lock_protocol::RETRY;
  }else{
    printf("OK\n");
    ret = lock_protocol::OK;
    lock_map[lid] = lock_server::locked;
  }
  pthread_mutex_unlock(&mutex);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("release request from clt %d\n", clt);

  pthread_mutex_lock(&mutex);
  lock_map[lid] = lock_server::free;
  pthread_mutex_unlock(&mutex);
  r = nacquire;
  return ret;
}


