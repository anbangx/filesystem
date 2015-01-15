// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include "lock_client_cache.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>

lock_release_impl::lock_release_impl(extent_client *ec) {
    this->ec = ec;
}

void
lock_release_impl::dorelease(lock_protocol::lockid_t lid) {
    ec->flush(lid);
}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  lu = new lock_release_impl(ec);
  lc = new lock_client_cache(lock_dst, lu);
  createroot();
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

std::string
yfs_client::filename(inum inum)
{
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
  if(inum & 0x80000000)
    return true;
  return false;
}

bool
yfs_client::isdir(inum inum)
{
  return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  int r = OK;
  // You modify this function for Lab 3
  // - hold and release the file lock

  printf("getfile %016llx\n", inum);
  lc->acquire(inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

  release:
    lc->release(inum);
  return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  int r = OK;
  // You modify this function for Lab 3
  // - hold and release the directory lock
  printf("getdir %016llx\n", inum);
  lc->acquire(inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

  release:
    lc->release(inum);
  return r;
}

int limited_rand(int limit)
{
  int r, d = RAND_MAX / limit;
  limit *= d;
  do { r = rand(); } while (r >= limit);
  return r / d;
}

yfs_client::inum
yfs_client::new_inum(bool isfile)
{
  yfs_client::inum finum;
  int rand_num = limited_rand(0x7FFFFFFF);
  if (isfile)
    finum = rand_num | 0x0000000080000000;
  else
    finum = rand_num & 0x000000007FFFFFFF;
  printf("new_inum %016llx \n",finum);
  return finum;
}

std::vector<yfs_client::dirent>
yfs_client::getSubfiles(std::string p_buf){
  std::vector<yfs_client::dirent> r_dirent;
  char *cstr, *p;
  std::string inum_buf;
  dirent curr_dirent;
  cstr = new char[p_buf.size() + 1];
  int count = 0;
  strcpy(cstr, p_buf.c_str());
  p = strtok (cstr, "/");
  while (p != NULL) {
    // Skip its own dir name & inum
    if(count < 2){
      p = strtok(NULL,"/");
      count++;
      continue;
    }
    if(count % 2 == 1) { // name
      curr_dirent.name = p;
      r_dirent.push_back(curr_dirent);
    }
    else { // ino
      inum_buf = p;
      curr_dirent.inum = n2i(inum_buf);
    }
    p = strtok(NULL,"/");
    count++;
  }

  delete[] cstr;
  return r_dirent;
}

bool
yfs_client::findfile(std::string p_buf, const char *name, inum &inum)
{
  char *cstr, *p;
  int count = 0;
  cstr = new char[p_buf.size() + 1];
  strcpy(cstr, p_buf.c_str());
  p = strtok (cstr, "/");
  while (p != NULL) {
    // Skip its own dir name & inum
    if(count < 2){
      p = strtok(NULL,"/");
      count++;
      continue;
    }
    if(count % 2 == 1) {
       if((strlen(p) == strlen(name)) && (!strncmp(p, name, strlen(name)))) {
         delete[] cstr;
         return true;
      }
    } else
      inum = n2i(p);
    p = strtok(NULL,"/");
    count++;
  }

  delete[] cstr;
  return false;
}

int
yfs_client::lookup(inum p_inum, const char *name, inum &c_inum){
  int r = NOENT;
  std::string p_buf;
  lc->acquire(p_inum);
  // Read Parent Dir and check if name already exists
  if (ec->get(p_inum, -1, 0, p_buf) != extent_protocol::OK) {
     printf("YFS_Client::lookup %016llx parent dir not exist\n", p_inum);
     r = NOENT;
     goto release;
  }

  if(findfile(p_buf, name, c_inum) == true){
    r = EXIST;
    goto release;
  }

  r = NOENT;
  release:
    lc->release(p_inum);
  return r;
}

int yfs_client::createroot()
{
  printf("YFS_Client::createroot\n");
  int r = OK;
  inum root_inum = 0x00000001;
  std::string file_buf = '/' + filename(root_inum) + "/root";
  printf("file_buf append: %s\n", file_buf.c_str());

  // flush into server
  lc->acquire(root_inum);
  if (ec->put(root_inum, -1, file_buf) != extent_protocol::OK){
     r = IOERR;
     goto release;
  }
  release:
    lc->release(root_inum);
  return r;
}

int
yfs_client::createfile(inum p_inum, const char *name, inum &c_inum, bool isfile)
{
  int r = OK;
  std::string p_buf;
  inum file_inum = new_inum(isfile);
  std::string file_buf("");
  inum inum;
  lc->acquire(p_inum);
  bool childlock = false;
  // Read Parent Dir and check if name already exists
  if (ec->get(p_inum, -1, 0, p_buf) != extent_protocol::OK) {
     printf("YFS_Client::createfile %016llx parent dir not exist\n", p_inum);
     r = NOENT;
     goto release;
  }
  printf("YFS_Client::createfile %016llx parent dir return succeeds, p_buf: %s\n", p_inum, p_buf.c_str());

  if(findfile(p_buf, name, inum) == true){
    r = EXIST;
    goto release;
  }

  if(!isfile)
    file_buf.append('/' + filename(file_inum) + "/" + name);
  // Create an empty extent for ino
  lc->acquire(p_inum);
  childlock = true;
  if (ec->put(file_inum, -1, file_buf) != extent_protocol::OK) {
     r = IOERR;
     goto release;
  }

  // Add a <name, ino> entry into @parent
  p_buf.append('/' + filename(file_inum) + "/" + name);
  if (ec->put(p_inum, -1, p_buf) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  c_inum = file_inum;
  release:
    lc->release(p_inum);
    if(childlock)
      lc->release(p_inum);
  return r;
}

void yfs_client::printdirent(std::vector<dirent> r_dirent){
  for(unsigned int i = 0; i < r_dirent.size(); i++){
    printf("YFS_Client::readdir %u. r_dirent[i].name=%s r_dirent[i].inum=%016llx\n", i, r_dirent[i].name.c_str(), r_dirent[i].inum);
  }
}

int
yfs_client::setattr(yfs_client::inum inum, int size)
{
  int r = OK;
  std::string buf;
  lc->acquire(inum);
  if (ec->put(inum, size, buf) != extent_protocol::OK) {
    r = NOENT;
    goto release;
  }
  release:
    lc->release(inum);
  return r;
}

int
yfs_client::readdir(inum p_inum, std::vector<dirent> &r_dirent){
  int r = OK;
  std::string p_buf;
  lc->acquire(p_inum);
  // Read Parent Dir and check if name already exists
  if (ec->get(p_inum, -1, 0, p_buf) != extent_protocol::OK) {
     printf("YFS_Client::readdir %016llx parent dir not exist\n", p_inum);
     r = NOENT;
     goto release;
  }
  printf("YFS_Client::readdir parent %016llx return succeeds, p_buf %s\n", p_inum, p_buf.c_str());

  r_dirent = getSubfiles(p_buf);
  r = OK;
  printdirent(r_dirent);
  release:
    lc->release(p_inum);
  return r;
}

int
yfs_client::read(inum inum, int offset, unsigned int size, std::string &buf)
{
  int r = OK;
  lc->acquire(inum);
  if(ec->get(inum, offset, size, buf) != extent_protocol::OK){
    printf("YFS_Client::read %016llx file not exist\n", inum);
    r = NOENT;
    goto release;
  }
  printf("YFS_Client::read %016llx file read success, buf %s\n", inum, buf.c_str());
  release:
    lc->release(inum);
  return r;
}

int
yfs_client::write(inum inum, int offset, unsigned int size, const char *buf)
{
  int r = OK;
  std::string file_buf;
//  size = size < buf.size() ? size : buf.size();
//  if(size == 0)
//    goto release;
  file_buf.append(buf, size);
  lc->acquire(inum);
  if(ec->put(inum, offset, file_buf) != extent_protocol::OK){
    printf("YFS_Client::write %016llx file not exist\n", inum);
    r = NOENT;
    goto release;
  }
  printf("YFS_Client::write %016llx success\n", inum);
  release:
    lc->release(inum);
  return r;
}

int
yfs_client::unlink(inum p_inum, const char *name)
{
  int r = OK;
  std::string p_buf;
  inum inum;
  std::string unlink_buf;
  bool childlock = false;
  lc->acquire(p_inum);
  // Read Parent Dir and check if name already exists
  if (ec->get(p_inum, -1, 0, p_buf) != extent_protocol::OK) {
     printf("YFS_Client::remove %016llx parent dir not exist\n", p_inum);
     r = NOENT;
     goto release;
  }
  printf("YFS_Client::remove %016llx parent dir return succeeds, p_buf: %s\n", p_inum, p_buf.c_str());

  if(findfile(p_buf, name, inum) == false){
    r = ENOENT;
    goto release;
  }
  lc->acquire(inum);
  if(ec->remove(inum) != extent_protocol::OK){
    printf("YFS_Client::remove %016llx file not exist\n", inum);
    r = IOERR;
    goto release;
  }
  //update parent dir entry
  unlink_buf = "/" + filename(inum) + "/" + name;
  p_buf.erase(p_buf.find(unlink_buf), unlink_buf.length());

  if (ec->put(p_inum, -1, p_buf) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  printf("YFS_Client::remove %016llx success\n", inum);
  release:
    lc->release(p_inum);
    if(childlock)
      lc->release(inum);
  return r;
}

