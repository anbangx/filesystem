// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include "extent_protocol.h"

class extent_server {

 public:
  struct extent_value {
    std::string data;
    extent_protocol::attr ext_attr;
  };

  std::map<extent_protocol::extentid_t, extent_value *> extent_store;
  extent_server();

  int put(extent_protocol::extentid_t id, std::string, int &);
  int get(extent_protocol::extentid_t id, std::string &);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int setattr(extent_protocol::extentid_t id, extent_protocol::attr a, bool sizeChanged);
  int remove(extent_protocol::extentid_t id, int &);
};

#endif 







