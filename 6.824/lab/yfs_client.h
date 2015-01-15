#ifndef yfs_client_h
#define yfs_client_h

#include <string>
//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>

#include "lock_protocol.h"
#include "lock_client_cache.h"

class lock_release_impl : public lock_release_user {
    private:
      class extent_client *ec;
    public:
      lock_release_impl(extent_client *ec);
      virtual ~lock_release_impl() {};
      void dorelease(lock_protocol::lockid_t lid);
};

class yfs_client {
  extent_client *ec;
  lock_release_impl *lu;
  lock_client_cache *lc;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    yfs_client::inum inum;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);
 public:

  yfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);

  int createroot();
  int createfile(inum p_inum, const char *name, inum &c_inum, bool isfile);
  int lookup(inum p_inum, const char *name, inum &c_inum);
  int readdir(inum p_inum, std::vector<dirent> &r_dirent);
  void printdirent(std::vector<dirent> r_dirent);
  std::vector<dirent> getSubfiles(std::string p_buf);
  bool findfile(std::string p_buf, const char *name, inum &inum);

  inum new_inum(bool isfile);

  int setattr(inum inum, int size);
  int read(inum inum, int offset, unsigned int size, std::string &buf);
  int write(inum inum, int offset, unsigned int size, const char *buf);
  int unlink(inum p_inum, const char *name);
};

#endif 
