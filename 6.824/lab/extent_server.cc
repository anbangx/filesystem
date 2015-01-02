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
  printf("Extent_Server::put - id %llu, offset %d, buf %s\n", id, offset, buf.c_str());
  if(offset == -1){
    printf("offset == -1");
    extent_value *p = new extent_value();
    p->data = buf;
    extent_store[id] = p;
    printf("Extent_Server::put - create new file/folder: %s\n", buf.c_str());
  } else{
    int size = buf.size();
    std::string data = extent_store[id]->data;
    if(size > 0){
      unsigned int oldSize = data.size();
      if(offset + size > oldSize){
        data.resize(offset + size);
      }
      data.replace(offset, size, buf);
      printf("Extent_Server::put - put data");
    } else{
      printf("Extent_Server::put - setattr");
      data.resize(offset);
    }
  }
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, int offset, unsigned int size, std::string &buf)
{
  printf("Extent_Server::get - id %llu, offset %d, size %d\n", id, offset, size);
  // You fill this in for Lab 2.
  if(extent_store.find(id) != extent_store.end()){
    std::string data = extent_store[id]->data;

    int totalSize = buf.size();
    if(offset == -1) // case1: read all data
      buf = data;
    else if(offset < totalSize){
      if(offset + size < totalSize)
        buf = data.substr(offset);
      else
        buf = data.substr(offset, size);
    }
    printf("Extent_Server::get - id %llu, get buf %s\n", buf.c_str());
    return extent_protocol::OK;
  } else{
    printf("Extent_Server::get - id %llu, value is empty.\n", id);
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

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  // You fill this in for Lab 2.
  extent_store.erase(id);
  return extent_protocol::IOERR;
}

