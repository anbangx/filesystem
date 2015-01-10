#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>

#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"


class lock_server_cache {
 private:
  int nacquire;
 protected:
   enum lock_state { LOCKFREE, LOCKED, REVOKING, RETRYING };
   struct lock_cache_value {
     lock_state l_state;
     std::string owner_clientid; // used to send revoke
     std::string retrying_clientid; // used to match with incoming acquire request
     std::list<std::string> waiting_clientids; // need to send retry
   };
   typedef std::map<lock_protocol::lockid_t, lock_cache_value*> TLockStateMap;
   TLockStateMap tLockMap;
   struct client_info {
       std::string client_id;
       lock_protocol::lockid_t lid;
   };
   std::list<client_info> retry_list;
   std::list<client_info> revoke_list;
   lock_cache_value* get_lock_obj(lock_protocol::lockid_t lid);
   pthread_mutex_t lmap_mutex;
   pthread_cond_t lmap_state_cv;
   pthread_mutex_t retry_mutex;
   pthread_cond_t retry_cv;
   pthread_mutex_t releaser_mutex;
   pthread_cond_t releaser_cv;


 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
