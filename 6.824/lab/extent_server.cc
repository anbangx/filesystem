// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <tprintf.h>

extent_server::extent_server()
{
  VERIFY(pthread_mutex_init(&extstore_mutex,0) == 0);
}

extent_server::~extent_server() {
   VERIFY(pthread_mutex_destroy(&extstore_mutex) == 0);
}

int extent_server::put(extent_protocol::extentid_t id, int offset, std::string buf, int &r)
{
  r = extent_protocol::OK;
//  ScopedLock ml(&extstore_mutex);
  extent_value *extent_obj;
  if (extent_store.count(id) <= 0)
     extent_obj = new extent_value();
  else
     extent_obj = extent_store[id];

  if (offset < 0)
     extent_obj->data = buf;
  else {
     if (offset > extent_obj->ext_attr.size) {
       extent_obj->data.resize(offset);
       extent_obj->data.append(buf);
     }
     else if (buf != "")
       extent_obj->data.replace(offset,buf.size(), buf);
     else
       extent_obj->data.resize(offset);
  }
  extent_obj->ext_attr.mtime = extent_obj->ext_attr.ctime = time(NULL);
  extent_obj->ext_attr.size = extent_obj->data.size();
  extent_store[id] = extent_obj;
  tprintf("extent_server::put, extent_store: id %016llx, buf %s\n", id, buf.c_str());

  //return extent_protocol::IOERR;
  return extent_protocol::OK;
//  printf("Extent_Server::put id %016llx, offset %d, buf %s\n", id, offset, buf.c_str());
//  // You fill this in for Lab 2.
//  r = extent_protocol::OK;
//  extent_value *extent_obj;
//  if (extent_store.count(id) <= 0)
//    extent_obj = new extent_value();
//  else
//    extent_obj = extent_store[id];
//
//  if (offset < 0)
//    extent_obj->data = buf;
//  else{
//    if (offset > extent_obj->ext_attr.size){
//      extent_obj->data.resize(offset);
//      extent_obj->data.append(buf);
//    } else if (buf != "")
//      extent_obj->data.replace(offset, buf.size(), buf);
//    else
//      extent_obj->data.resize(offset);
//  }
//  extent_obj->ext_attr.mtime = extent_obj->ext_attr.ctime = time(NULL);
//  extent_obj->ext_attr.mtime = extent_obj->ext_attr.mtime = time(NULL);
//  extent_obj->ext_attr.size = extent_obj->data.size();
//  extent_store[id] = extent_obj;
//  printf("Extent_Server::put succeeds, id %016llx, buf %s\n", id, buf.c_str());
//  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, int offset, unsigned int size, std::string &buf)
{
  printf("Extent_Server::get - id %016llx, offset %d, size %u\n", id, offset, size);
  // You fill this in for Lab 2.
  if(extent_store.find(id) != extent_store.end()){
    std::string data = extent_store[id]->data;
    int totalSize = data.size();
    if(offset == -1) // case1: read all data
      buf = data;
    else if(offset < totalSize){
      if(offset + size < totalSize)
        buf = data.substr(offset, size);
      else
        buf = data.substr(offset);
    }
    printf("Extent_Server::get - id %016llx, get buf %s, buf.size %u\n", id, buf.c_str(), buf.size());
    return extent_protocol::OK;
  } else{
    printf("Extent_Server::get - id %016llx, value is empty.\n", id);
    return extent_protocol::NOENT;
  }
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  // You fill this in for Lab 2.
  // You replace this with a real implementation. We send a phony response
  // for now because it's difficult to get FUSE to do anything (including
  // unmount) if getattr fails.
  printf("Extent_Server::getattr - id %016llx", id);
  extent_value *extent_obj;
  ScopedLock ml(&extstore_mutex);
  if (extent_store.count(id) > 0) {
    tprintf("extent_server::getattr returning from store for %016llx \n", id);
    extent_obj = extent_store[id];
    a.size = extent_obj->ext_attr.size;
    a.atime = extent_obj->ext_attr.atime;
    a.mtime = extent_obj->ext_attr.mtime;
    a.ctime = extent_obj->ext_attr.ctime;
    printattr(a);
    return extent_protocol::OK;
  } else {
      //change once confident
      /*  a.size = 0;
          a.atime = 0;
          a.mtime = 0;
          a.ctime = 0;
          */
    return extent_protocol::IOERR;
  }
}

void extent_server::printattr(extent_protocol::attr a){
  printf("Extent_Server::getattr a.atime %u, a.ctime %u, a.mtime %u, a.size %u", a.atime, a.ctime, a.mtime, a.size);
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  // You fill this in for Lab 2.
  extent_store.erase(id);
  return extent_protocol::OK;
}

