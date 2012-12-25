#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <algorithm>
#include <map>
#include "../pf/pf.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

# define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum{ MIN = 0, MAX, SUM, AVG, COUNT } AggregateOp;



// The following functions use  the following 
// format for the passed data.
//    For int and real: use 4 bytes
//    For varchar: use 4 bytes for the length followed by
//                          the characters

struct Value {
    AttrType type;          // type of value               
    void     *data;         // value                       
};


struct Condition {
    string lhsAttr;         // left-hand side attribute                     
    CompOp  op;             // comparison operator                          
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string rhsAttr;         // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};


class Iterator {
    // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(vector<Attribute> &attrs) const = 0;
        virtual ~Iterator() {};
};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
        RM &rm;
        RM_ScanIterator *iter;
        string tablename;
        vector<Attribute> attrs;
        vector<string> attrNames;
        
        TableScan(RM &rm, const string tablename, const char *alias = NULL):rm(rm)
        {
            // Get Attributes from RM
            rm.getAttributes(tablename, attrs);

            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs[i].name);
            }
            // Call rm scan to get iterator
            iter = new RM_ScanIterator();
            rm.scan(tablename, "", NO_OP, NULL, attrNames, *iter);

            // Store tablename
            this->tablename = tablename;
            if(alias) this->tablename = alias;
        };
       
        // Start a new iterator given the new compOp and value
        void setIterator()
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tablename, "", NO_OP, NULL, attrNames, *iter);
        };
       
        RC getNextTuple(void *data)
        {
            RID rid;
            return iter->getNextTuple(rid, data);
        };
        
        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;
            
            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tablename;
                tmp += ".";
                tmp += attrs[i].name;
                attrs[i].name = tmp;
            }
        };
        
        ~TableScan() 
        {
            iter->close();
        };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
    public:
        RM &rm;
        IX_IndexScan *iter;
        IX_IndexHandle handle;
        string tablename;
        vector<Attribute> attrs;
        
        IndexScan(RM &rm, const IX_IndexHandle &indexHandle, const string tablename, const char *alias = NULL):rm(rm)
        {
            // Get Attributes from RM
            rm.getAttributes(tablename, attrs);
                     
            // Store tablename
            this->tablename = tablename;
            if(alias) this->tablename = string(alias);
            
            // Store Index Handle
            iter = NULL;
            this->handle = indexHandle;
        };
       
        // Start a new iterator given the new compOp and value
        void setIterator(CompOp compOp, void *value)
        {
            if(iter != NULL)
            {
                iter->CloseScan();
                delete iter;
            }
            iter = new IX_IndexScan();
            iter->OpenScan(handle, compOp, value);
        };
       
        RC getNextTuple(void *data)
        {
            RID rid;
            int rc = iter->GetNextEntry(rid);
            if(rc == 0)
            {
                rc = rm.readTuple(tablename.c_str(), rid, data);
            }
            return rc;
        };
        
        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tablename;
                tmp += ".";
                tmp += attrs[i].name;
                attrs[i].name = tmp;
            }
        };
        
        ~IndexScan() 
        {
            iter->CloseScan();
        };
};


class Filter : public Iterator {
    // Filter operator
    public:
        Filter(Iterator *input,                         // Iterator of input R
               const Condition &condition               // Selection condition 
        );
        ~Filter(){};
        
        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;

    private:
        Iterator *iter;
        Condition cond;
        vector<Attribute> attrs;
        //Attribute leftAttribute;


        void getAttributeOffsetAndLength(void*, int&, int&);
        bool isAttribute(char*, int);
};


class Project : public Iterator {
    // Projection operator
    public:
        Project(Iterator *input,                            // Iterator of input R
                const vector<string> &attrNames);           // vector containing attribute names
        ~Project(){};
        
        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;

    private:
        Iterator *iter;
       // Condition cond;
        vector<Attribute> attributes;
        vector<string> attrsNames;

        void getMaxLengthOfTuple(int&);
        void parseTuple(char*, char*);
};


class NLJoin : public Iterator {
    // Nested-Loop join operator
    public:
        NLJoin(Iterator *leftIn,                             // Iterator of input R
               TableScan *rightIn,                           // TableScan Iterator of input S
               const Condition &condition,                   // Join condition
               const unsigned numPages                       // Number of pages can be used to do join (decided by the optimizer)
        );
        ~NLJoin(){clearanceJob();};
        
        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;

    private:
        Iterator* iter;
        TableScan* scan;
        Condition cond;
        unsigned numOfPages;
        vector<Attribute> leftAttrs;
        vector<Attribute> rightAttrs;
        vector<Attribute> attrs;
        Attribute leftAttribute;
        list<char*> results;
        list<int> resultLengths;

        void clearanceJob();
        void addAttribute(vector<Attribute> &);
        void getMaxLengthOfTuple(vector<Attribute> &,int &);
        void getAttributeOffsetAndLength(vector<Attribute> &, string&, char*, int&, int&);
        void getAttribute(char*, char*, const int, const int);
        bool joinValues(char*, char*, const int, const int);
        void addResult(char*);
        void setResults();
        void getTupleLength(vector<Attribute>& , char* , int& );
        void joinTuples(char*, char*, char*);
        void addResultLength(int);
        void setAttribute();
};


class INLJoin : public Iterator {
    // Index Nested-Loop join operator
    public:
        INLJoin(Iterator *leftIn,                               // Iterator of input R
                IndexScan *rightIn,                             // IndexScan Iterator of input S
                const Condition &condition,                     // Join condition
                const unsigned numPages                         // Number of pages can be used to do join (decided by the optimizer)
        );
        
        ~INLJoin(){clearanceJob();};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;

    private:
        Iterator* iter;
        IndexScan* scan;
        Condition cond;
        unsigned numOfPages;
        vector<Attribute> leftAttrs;
        vector<Attribute> rightAttrs;
        vector<Attribute> attrs;
        list<char*> results;
        list< int> resultLengths;
        Attribute leftAttribute;

        void addAttribute(vector<Attribute> &);
        void getMaxLengthOfTuple(vector<Attribute> &,int &);
        void getAttributeOffsetAndLength(vector<Attribute> &, string&, char*, int&, int&);
        void getAttribute(char*, char*, const int, const int);
        bool joinValues(char*, char*, const int, const int);
        void addResult(char*);
        void setResults();
        void getTupleLength(vector<Attribute>& , char* , int& );
        void joinTuples(char*, char*, char*);
        void addResultLength(int);
        void clearanceJob();
        void setAttribute();
};


class HashJoin : public Iterator {
    // Hash join operator
    public:
        HashJoin(Iterator *leftIn,                                // Iterator of input R
                 Iterator *rightIn,                               // Iterator of input S
                 const Condition &condition,                      // Join condition
                 const unsigned numPages                          // Number of pages can be used to do join (decided by the optimizer)
        );
        
        ~HashJoin(){clearanceJob();};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;

    private:
        Iterator* leftIter;
        Iterator* rightIter;
        Condition cond;
        unsigned numOfPages;
        vector<Attribute> leftAttrs;
        vector<Attribute> rightAttrs;
        vector<Attribute> attrs;
        Attribute attribute;
        string leftPartitionName;
        string rightPartitionName;
        PF_Manager* pf;
        vector<char*> hashTable;
        vector<int> lengths;
        vector<char*> innerTable;
        vector<int> innerLengths;
        list<char*>results;
        list<int> resultLengths;
        int numberOfPartitions;

        void addAttribute(vector<Attribute>& );
        void hashPartition(Iterator *, int );
        void getMaxLengthOfTuple(vector<Attribute>& , int & );
        void getAttributeOffsetAndLength(vector<Attribute> & , string& , char* , int& , int &);
        void hashRecord(Iterator* , char* , int, int );
        void setAttribute();
        void deletePartitions(int);
        void createPartitions(int);
        void putRecordInFile(string ,char*, int length );
        void hashValue(char*, int&);
        void buildInMemoryHash(char*, int);
        void readLeftPartition(string);
        void matchRightPartition(string);
        void buildIn();
        void setResult();
        bool joinValues(char* left, char* right, const int llength, const int rlength);
        void getAttribute(vector<Attribute> & attrs, string& name,char* record, char* attr, int&);
        void joinTuples(char*, char*, char*);
        void addResult(char*, int);
        void joinTuples(char* left, char* right, int lTupleLength, int rTupleLength);
        void clearRight();
        void clearLeft();
        void clearanceJob();
        void initializePartition(string);
        void getTupleLength(vector<Attribute>&, char*, int&);
};


class Aggregate : public Iterator {
    // Aggregation operator
    public:
        Aggregate(Iterator *input,                              // Iterator of input R
                  Attribute aggAttr,                            // The attribute over which we are computing an aggregate
                  AggregateOp op                                // Aggregate operation
        );

        // Extra Credit
        Aggregate(Iterator *input,                              // Iterator of input R
                  Attribute aggAttr,                            // The attribute over which we are computing an aggregate
                  Attribute gAttr,                              // The attribute over which we are grouping the tuples
                  AggregateOp op                                // Aggregate operation
        );

        ~Aggregate(){clearanceJob();};
        
        RC getNextTuple(void *data);
        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrname = "MAX(rel.attr)"
        void getAttributes(vector<Attribute> &attrs) const;

    private:
        vector<Attribute> attributes;
        Iterator* iter;
        Attribute aggregateAttribute;
        Attribute groupAttribute;
        AggregateOp operation;
        vector<float>results;
        map<char*, float>maps;
        map<char*, float>counts;
        map<char*, int>lengths;
        map<char*,float>::iterator it;
        int flag;
        int button;

        void getOperation(string&) const;
        void getMaxLengthOfTuple(int&);
        void getAttributeOffsetAndLength(vector<Attribute>&attrs, string& name,  void* data, int &length, int& offset);
        void addAttribute(char*);
        void prepareResult();
        void prepareResult2();
        float sum();
        float average();
        float count();
        void getResult(void*);
        void prepareAttribute(char*);
        void setMap(char*, char*);
        void setIterator();
        void clearanceJob();
};

#endif
