
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <ostream>

#include "../pf/pf.h"

using namespace std;


// Return code
typedef int RC;


// Record ID
typedef struct
{
  unsigned pageNum;
  unsigned slotNum;
} RID;

// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};


// Comparison Operator
typedef enum { EQ_OP = 0,  // =
           LT_OP,      // <
           GT_OP,      // >
           LE_OP,      // <=
           GE_OP,      // >=
           NE_OP,      // !=
           NO_OP       // no condition
} CompOp;

typedef struct{int offset; int length;int type;}Data;

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through records
// The way to use it is like the following:
//  RM_ScanIterator rmScanIterator;
//  rm.open(..., rmScanIterator);
//  while (rmScanIterator(rid, data) != RM_EOF) {
//    process the data;
//  }
//  rmScanIterator.close();
class RM;
class RM_ScanIterator {
public:
  RM_ScanIterator() { i = 0;};
  ~RM_ScanIterator() {};

  // "data" follows the same format as RM::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close() { return -1; };

  vector<RID> rids;
  string tableName;
  RM *rm ;
  void getRM(RM *);
  vector<int> recordLengths;
  vector<string>attributeNames;

private:
  int i;
 };


// Record Manager
class RM
{
public:
  static RM* Instance();

  RC createTable(const string tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string tableName);

  RC getAttributes(const string tableName, vector<Attribute> &attrs);

  //  Format of the data passed into the function is the following:
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!!The same format is used for updateTuple(), the returned data of readTuple(), and readAttribute()
  RC insertTuple(const string tableName, const void *data, RID &rid);

  RC deleteTuples(const string tableName);

  RC deleteTuple(const string tableName, const RID &rid);

  // Assume the rid does not change after update
  RC updateTuple(const string tableName, const void *data, const RID &rid);

  RC readTuple(const string tableName, const RID &rid, void *data);

  RC readAttribute(const string tableName, const RID &rid, const string attributeName, void *data);

  RC reorganizePage(const string tableName, const unsigned pageNumber);

  // scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(const string tableName,
      const string conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);


// Extra credit
public:
  RC dropAttribute(const string tableName, const string attributeName);

  RC addAttribute(const string tableName, const Attribute attr);

  RC reorganizeTable(const string tableName);


  RC readAttributes(const string tableName, const RID &rid, vector<string> attributeName, void *data);

protected:
  RM();
  ~RM();

  /*
   *  Create a catalog to hold all information about your database. This includes at least the following:
    Table info (name).
    For each table, the columns, and for each of these columns: the column name, type and length
    The name of the paged file in which the data corresponding to each table is stored.
   */
private:
  static RM *_rm;
  //string tableName;
  //vector<Attribute> attrs;
  //string fileName;
  PF_Manager *pf ;
  //PF_FileHandle tableHandle;
  //PF_FileHandle catalogHandle;
  //map <string , vector<Attribute> > tables;
  //int tableID;

  RC createCatalog();
  RC insertTableInfo(const string tableName, const vector<Attribute> &attrs);				//insert table info in catalog
  RC deleteTableInfo(string tableName);         //delete table info from catalog
  void prepareTuple(const string tableName, const void* data, void * tuple);
  int getDataLength(const string tableName, const void* data);
  void prepareNewPage(void* data);
  void prepareNewDirectory(void* data);


};

#endif
