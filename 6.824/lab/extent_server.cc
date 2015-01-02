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
  printf("Extent_Server::put - id %016lx, offset %d, buf %s, buf.size %u\n", id, offset, buf.c_str(), buf.size());
  if(offset == -1){
    extent_value *p = new extent_value();
    p->data = buf;
    extent_store[id] = p;
    printf("Extent_Server::put - create new file/folder: %s\n", extent_store[id]->data.c_str());
  } else{
    int size = buf.size();
    std::string data = extent_store[id]->data;
    if(size > 0){
      printf("Extent_Server::put - data1 %s\n", data.c_str());
      unsigned int oldSize = data.size();
      if(offset + size > oldSize){
        data.resize(offset + size);
      }
      printf("Extent_Server::put - data2 %s\n", data.c_str());
      data.replace(offset, size, buf);
      printf("Extent_Server::put - put data succeeds.\n");
      extent_store[id]->data = data;
      printf("Extent_Server::put - now id %016lx, extent_store[id]->data %s\n", id, extent_store[id]->data.c_str());
    } else{
      printf("Extent_Server::put - setattr\n");
      data.resize(offset);
    }
  }
  extent_store[id]->ext_attr.size = extent_store[id]->data.size();
  printf("Extent_Server::put - set ext_attr.size %u\n", extent_store[id]->ext_attr.size);
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, int offset, unsigned int size, std::string &buf)
{
  printf("Extent_Server::get - id %016lx, offset %d, size %u\n", id, offset, size);
  // You fill this in for Lab 2.
  if(extent_store.find(id) != extent_store.end()){
    std::string data = extent_store[id]->data;
    printf("Extent_Server::get - id %016lx, data %s\n", id, data.c_str());
    int totalSize = data.size();
    if(offset == -1) // case1: read all data
      buf = data;
    else if(offset < totalSize){
      if(offset + size < totalSize){
        printf("Extent_Server::get offset + size < totalSize\n");
        buf = data.substr(offset);
      }
      else{
        buf = data.substr(offset, size);
      }
    }
    printf("Extent_Server::get - id %016lx, get buf %s, buf.size %u\n", id, buf.c_str(), buf.size());
    return extent_protocol::OK;
  } else{
    printf("Extent_Server::get - id %016lx, value is empty.\n", id);
    return extent_protocol::NOENT;
  }
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  // You fill this in for Lab 2.
  // You replace this with a real implementation. We send a phony response
  // for now because it's difficult to get FUSE to do anything (including
  // unmount) if getattr fails.
  a = extent_store[id]->ext_attr;
  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  // You fill this in for Lab 2.
  extent_store.erase(id);
  return extent_protocol::IOERR;
}

