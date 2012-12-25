
#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rm/rm.h"
#include "../pf/pf.h"
#include <algorithm>
#include <list>

# define IX_EOF (-1)  // end of the index scan

using namespace std;

class IX_IndexHandle;

class IX_Manager {
 public:
  static IX_Manager* Instance();

  RC CreateIndex(const string tableName,       // create new index
		 const string attributeName);
  RC DestroyIndex(const string tableName,      // destroy an index
		  const string attributeName);
  RC OpenIndex(const string tableName,         // open an index
	       const string attributeName,
	       IX_IndexHandle &indexHandle);
  RC CloseIndex(IX_IndexHandle &indexHandle);  // close index
  
 protected:
  IX_Manager   (){pf = PF_Manager::Instance();};                             // Constructor
  ~IX_Manager  (){};                             // Destructor
 
 private:
  static IX_Manager *_ix_manager;
  //
  PF_Manager *pf ;
  void prepareIndexFile(const string );
  void initializeTree(const string);
};


class IX_IndexHandle {
 public:
  IX_IndexHandle  (){ newChildEntry = NULL;  rm = RM::Instance(); };                           // Constructor
  ~IX_IndexHandle (){};                           // Destructor

  // The following two functions are using the following format for the passed key value.
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  RC InsertEntry(void *key, const RID &rid);  // Insert new index entry
  RC DeleteEntry(void *key, const RID &rid);  // Delete index entry

  //**
  void setHandle(PF_FileHandle handle){indexHandle = handle;};
  PF_FileHandle& getHandle(){return indexHandle;};
  void getTreeInfo();
  void setAttributeName(string a){attributeName = a;};
  void setTableName(string t){tableName = t;};
  void setAttribute(string tableName, string attributeName);
  void scan(CompOp , void*, list<RID>& ) ;
  bool isOpen();
  int getTreeHeight(){return treeHeight;};

 protected:
  void setTreeHeight(int h){treeHeight = h;};
  void setFanOut(RID);
  void setRootPage(int r){rootPageNumber = r; currentPageNumber = rootPageNumber;};
  int getMaxKeyNumber(){return 2*d_fanout;};
  int getFreeSpace(char* );
  int getEntrySize(RID);


  void prepareEntry(char*,void*key, const RID);
  void updatePage(char*, char*, const int, const int);
  int compareKeys(char*, char*);
  void setNodeHeight(char*,const int);
  void updateNodeHeight(const int );
  void createNewRoot(char*, char*, const int);
  void setSiblings(char*, const int, const int);
  void splitPage(char*, char*, char*, char*);
  bool isRoot(const int);
  bool isLeaf(char* );
  int findSubTree(void*, char*);
  void resetCurrentPagePointer();
  void getSiblings(char*, int&, int&);
  int find(char*, void*);
  void getRID(char* , RID& );
  void scanOnePage(char* node, CompOp operation, void* value, list<RID>& ids) ;
  //bool deletion(char*, char*, char*, char*);
  bool isEmpty(char*);
  bool remove(char*, char*);
  //void getScanFileName(string&)const;

 private:
  PF_FileHandle indexHandle;
  //RC insert(char* nodePage,const char* entry, char* newchildentry);
  int treeHeight;
  int rootPageNumber;
  int currentPageNumber;
  int d_fanout;
  RM *rm;
  string attributeName;
  Attribute attribute;
  string tableName;
  char* newChildEntry;
  char* oldChildEntry;
  vector<int> freePages;

};

class IX_IndexScan {
 public:
  IX_IndexScan(){handle = new IX_IndexHandle();};  								// Constructor
  ~IX_IndexScan(){delete handle;}; 								// Destructor

  // for the format of "value", please see IX_IndexHandle::InsertEntry()
  RC OpenScan(const IX_IndexHandle &indexHandle, // Initialize index scan
	      CompOp      compOp,
	      void        *value);           

  RC GetNextEntry(RID &rid);  // Get next matching entry
  RC CloseScan();             // Terminate index scan

 private:
  list<RID> ids;
  IX_IndexHandle* handle;
  //void setIX(const IX_IndexHandle& ix)const{handle =ix;} ;
};

// print out the error message for a given return code
void IX_PrintError (RC rc);



#endif
