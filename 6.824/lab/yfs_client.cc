// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>


yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
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

  printf("getfile %016llx\n", inum);
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

  return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  int r = OK;

  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

  release:
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
//  else
//  finum = rand_num & 0x000000007FFFFFFF;
  printf("new_inum %016llx \n",finum);
  return finum;
}

int
yfs_client::lookup(inum p_inum, const char *name, inum &c_inum){
  int r = OK;
  std::string p_buf, inum_buf;
  char *cstr, *p;
  int count = 0;
  inum curr_inum;
  // Read Parent Dir and check if name already exists
  if (ec->get(p_inum, -1, 0, p_buf) != extent_protocol::OK) {
     printf("yfs_client::createfile %016llx parent dir not exist\n", p_inum);
     r = NOENT;
     goto release;
  }

  cstr = new char[p_buf.size() + 1];
  strcpy(cstr, p_buf.c_str());
  p = strtok (cstr, "/");
  while (p != NULL) {
    printf("createfile: p %c\n", *p);
    // Skip its own dir name & inum
    if(count != 1 && count % 2 == 1) {
       if((strlen(p) == strlen(name)) && (!strncmp(p, name, strlen(name)))) {
         delete[] cstr;
         r = EXIST;
         c_inum = curr_inum;
         goto release;
      }
    } else{
      inum_buf = p;
      curr_inum = n2i(inum_buf);
    }
    p = strtok(NULL,"/");
    count++;
  }

  delete[] cstr;
  r = NOENT;
  release:
  return r;
}

int
yfs_client::createfile(inum p_inum, const char *name, inum &c_inum, bool isfile)
{
  int r = OK;
  std::string p_buf;
  char *cstr, *p;
  int count = 0;
  inum file_inum = new_inum(isfile);
  std::string file_buf("");
  // Read Parent Dir and check if name already exists
  if (ec->get(p_inum, -1, 0, p_buf) != extent_protocol::OK) {
     printf("yfs_client::createfile %016llx parent dir not exist\n", p_inum);
     r = NOENT;
     goto release;
  }

  cstr = new char[p_buf.size() + 1];
  strcpy(cstr, p_buf.c_str());
  p = strtok (cstr, "/");
  while (p != NULL) {
    printf("createfile: p %c\n", *p);
    // Skip its own dir name & inum
    if(count != 1 && count % 2 == 1) {
       if((strlen(p) == strlen(name)) && (!strncmp(p, name, strlen(name)))) {
         delete[] cstr;
         r = EXIST;
         goto release;
      }
    }
    p = strtok(NULL,"/");
    count++;
  }

  delete[] cstr;
  if(!isfile)
    file_buf.append('/' + filename(file_inum) + "/" + name);
  //Create an empty extent for ino
  if (ec->put(file_inum, -1, file_buf) != extent_protocol::OK) {
     r = IOERR;
     goto release;
  }
  //Add a <name, ino> entry into @parent
  p_buf.append('/' + filename(file_inum) + "/" + name);
  if (ec->put(p_inum,-1, p_buf) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  c_inum = file_inum;
  release:
  printf("release");
  return r;
}

int
yfs_client::readdir(inum p_inum, std::vector<dirent> &r_dirent){
  int r = OK;
  std::string p_buf, inum_buf;
  char *cstr, *p;
  int count = 0;
  dirent curr_dirent;
  // Read Parent Dir and check if name already exists
  if (ec->get(p_inum, -1, 0, p_buf) != extent_protocol::OK) {
     printf("yfs_client::createfile %016llx parent dir not exist\n", p_inum);
     r = NOENT;
     goto release;
  }

  cstr = new char[p_buf.size() + 1];
  strcpy(cstr, p_buf.c_str());
  p = strtok (cstr, "/");
  while (p != NULL) {
    printf("createfile: p %c\n", *p);
    // Skip its own dir name & inum
    if(count && count!=1) {
      if(count % 2 == 1) { // name
        curr_dirent.name = p;
        r_dirent.push_back(curr_dirent);
      }
      else { // ino
        inum_buf = p;
        curr_dirent.inum = n2i(inum_buf);
      }
    }
    p = strtok(NULL,"/");
    count++;
  }

  delete[] cstr;
  r = OK;
  release:
  return r;
}


