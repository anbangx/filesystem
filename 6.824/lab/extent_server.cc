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

int extent_server::put(extent_protocol::extentid_t id, int offset, std::string buf, int &r)
{
  // You fill this in for Lab 2.
  printf("Extent_Server::put - key %llu enter, buf: %s\n", id, buf.c_str());
  if(offset == -1){
    extent_value *p = new extent_value();
    p->data = buf;
    extent_store[id] = p;
  } else{
    int size = buf.size();
    std::string data = extent_store[id]->data;
    if(size > 0){
      unsigned int oldSize = data.size();
      if(offset + size > oldSize){
        data.resize(offset + size);
      }
      data.replace(offset, size, buf);
    } else{
      data.resize(offset);
    }
  }
  printf("Extent_Server::put - key %llu enter, extent_store[id]->data: %s\n", id, extent_store[id]->data.c_str());
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, int offset, unsigned int size, std::string &buf)
{
  // You fill this in for Lab 2.
  if(extent_store.find(id) != extent_store.end()){
    std::string data = extent_store[id]->data;
    printf("Extent_Server::get - key %llu, get value: %s\n", id, data.c_str());

    int totalSize = buf.size();
    if(offset == -1) // case1: read all data
      buf = data;
    else if(offset < totalSize){
      if(offset + size < totalSize)
        buf = data.substr(offset);
      else
        buf = data.substr(offset, size);
    }
    return extent_protocol::OK;
  } else{
    printf("Extent_Server::get - key %llu, value is empty.\n", id);
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

//int extent_server::setattr(extent_protocol::extentid_t id, int size, std::string &buf)
//{
//  if(extent_store.find(id) == extent_store.end())
//    return extent_protocol::NOENT;
//  extent_value *p = extent_store[id];
//  int oldSize = p->ext_attr.size;
//  if(oldSize < size){
//    std::string data = p->data;
//    for(int i = oldSize; i < size; i++)
//      data[i] = '\0';
//  }
//  return extent_protocol::OK;
//}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  // You fill this in for Lab 2.
  extent_store.erase(id);
  return extent_protocol::IOERR;
}

