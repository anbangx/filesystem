// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>


lock_server::lock_server():
  nacquire (0)
{
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
  printf("Server acquire\n");
  lock_protocol::status ret;
  printf("stat request from clt %d\n", clt);

  if(lock_map.find(lid) == lock_map.end()){
    printf("OK");
    ret = lock_protocol::OK;
    lock_map[lid] = lock_server::locked;
  }else{
    printf("RETRY");
    ret = lock_protocol::RETRY;
  }
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);

  lock_map[lid] = lock_server::free;
  r = nacquire;
  return ret;
}


