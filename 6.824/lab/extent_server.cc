// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extent_server::extent_server()
{
}


int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  // You fill this in for Lab 2.
  printf("Extent_Server::put - key %d enter, buf: %s\n", id, buf.c_str());
  extent_value *p = new extent_value();
  p->data = buf;
  extent_store[id] = p;
  printf("Extent_Server::put - key %d enter, extent_store[id]->data: %s\n", id, extent_store[id]->data.c_str());
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  // You fill this in for Lab 2.
  printf("Extent_Server::get - key %d enter\n", id);
  if(extent_store.find(id) != extent_store.end()){
    buf = extent_store[id]->data;
    printf("Extent_Server::get - key %d, get value: %s\n", id, buf.c_str());
    return extent_protocol::OK;
  } else{
    printf("Extent_Server::get - key %d, value is empty.\n", id);
    return extent_protocol::NOENT;
  }
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  // You fill this in for Lab 2.
  // You replace this with a real implementation. We send a phony response
  // for now because it's difficult to get FUSE to do anything (including
  // unmount) if getattr fails.
  a.size = 0;
  a.atime = 0;
  a.mtime = 0;
  a.ctime = 0;
  return extent_protocol::OK;
}

int extent_server::setattr(extent_protocol::extentid_t id, extent_protocol::attr a, bool sizeChanged)
{
  extent_value *p = new extent_value();
  int oldSize = p->ext_attr.size;
  p->ext_attr = a;
  extent_store[id] = p;
  if(sizeChanged && oldSize < a.size){
    std:string data = p->data;
    for(int i = oldSize; i < a.size; i++)
      data[i] = '\0';
  }
  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  // You fill this in for Lab 2.
  extent_store.erase(id);
  return extent_protocol::IOERR;
}

