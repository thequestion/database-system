#include <stdio.h>
#include <string.h>
# include "qe.h"

 Filter::Filter(Iterator *input,                         // Iterator of input R
               const Condition &condition               // Selection condition
        ):iter(input), cond(condition)
 {
	 iter->getAttributes(attrs);
 }

RC Filter::getNextTuple(void* data)
{

	if(cond.bRhsIsAttr == true)
		return QE_EOF;
	while(	iter->getNextTuple(data) == 0	)
	{
		int offset = 0;
		int lengthOfData = 0;
		getAttributeOffsetAndLength(data, offset, lengthOfData);
		char* attribute = new char[lengthOfData];
		memcpy(attribute, (char*)data+offset, lengthOfData);
		//getAttribute(data, offset, lengthOfData);
		if( isAttribute(attribute, lengthOfData) )
		{
			delete[]attribute;
			return 0;
		}
		delete[]attribute;
	}
	return QE_EOF;
}


void Filter::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	attrs = this->attrs;
}

/*
 * tell if the attribute match the given condition
 */
bool Filter::isAttribute(char* attribute, int length)
{
	if( cond.rhsValue.type == TypeInt || cond.rhsValue.type == TypeReal ){
		float attr;
		float value;
		memcpy(&attr, attribute, sizeof(int));
		memcpy(&value, (char*)cond.rhsValue.data, sizeof(int));
		switch(cond.op)
		{
		case 0:
			if(attr == value)
				return true;
			break;
		case 1:
			if(attr < value)
				return true;
			break;
		case 2:
			if(attr > value)
				return true;
			break;
		case 3:
			if(attr <= value)
				return true;
			break;
		case 4:
			if(attr >= value)
				return true;
			break;
		case 5:
			if(attr != value)
				return true;
			break;
		default:
			return true;
			break;
		}
	}else{
		switch(cond.op)
		{
		case 0:
			if(memcmp(attribute, cond.rhsValue.data+sizeof(int), length) == 0)
				return true;
			break;
		case 1:
			if(memcmp(attribute, cond.rhsValue.data+sizeof(int), length) < 0)
				return true;
			break;
		case 2:
			if(memcmp(attribute, cond.rhsValue.data+sizeof(int), length) > 0)
				return true;
			break;
		case 3:
			if(memcmp(attribute, cond.rhsValue.data+sizeof(int), length) <= 0)
				return true;
			break;
		case 4:
			if(memcmp(attribute, cond.rhsValue.data+sizeof(int), length) >= 0)
				return true;
			break;
		case 5:
			if(memcmp(attribute, cond.rhsValue.data+sizeof(int), length) != 0)
				return true;
			break;
		default:
			return true;
			break;
		}
	}
	return false;
}

/*
 * get the attribute offset and length in the tuple
 */
void Filter::getAttributeOffsetAndLength(void* data, int &offset, int& length)
{
		for(unsigned i = 0; i < attrs.size(); i++)
		{
			if( strcmp(attrs[i].name.c_str(), cond.lhsAttr.c_str() ) == 0){
				if( attrs[i].type == TypeInt || attrs[i].type == TypeReal )
					length = sizeof(int);
				else{
					memcpy(&length,(char*) data+offset, sizeof(int));
					offset += sizeof(int);
				}

				break;
			}
			else{
				if( attrs[i].type == TypeInt || attrs[i].type == TypeReal ){
					offset += sizeof(int);
				}
				else{
					int stringLength = 0;
					memcpy(&stringLength,(char*)data+offset, sizeof(int));
					offset += (stringLength+sizeof(int));
				}
			}
		}
}

Project::Project(Iterator *input,                            // Iterator of input R
        const vector<string> &attrNames):iter(input), attrsNames(attrNames)
{
    	iter->getAttributes(attributes);
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	for(unsigned i = 0; i < attributes.size(); i++)
	{
		for(unsigned j = 0; j < attrsNames.size(); j++)
		{
			if(strcmp(attributes[i].name.c_str(), attrsNames[j].c_str()) == 0)
				attrs.push_back(attributes[i]);
		}
	}
}

RC Project::getNextTuple(void* data)
{
	int maxLengthOfTuple = 0;
	getMaxLengthOfTuple(maxLengthOfTuple);
	char* tuple = new char[maxLengthOfTuple];
	if( iter->getNextTuple(tuple)!=0)
	{
		delete[]tuple;
		return QE_EOF;
	}
	parseTuple(tuple, (char*)data);
	delete[]tuple;
	return 0;
}

void Project::parseTuple(char* tuple, char* data)
{
	int tupleOffset = 0;
	int dataOffset = 0;
	for(unsigned i = 0; i < attributes.size(); i++)
	{
		for(unsigned j = 0; j <= attrsNames.size(); j++)
		{
			if(j == attrsNames.size())
			{
				if(attributes[i].type == TypeInt|| attributes[i].type == TypeReal)
				{
					tupleOffset += sizeof(int);
				}
				else
				{
					int length = 0;
					memcpy(&length, tuple+tupleOffset, sizeof(int));
					tupleOffset += sizeof(int);
					tupleOffset += length;
				}
				break;
			}
			if( strcmp(attributes[i].name.c_str(), attrsNames[j].c_str()) == 0 )
			{
				if(attributes[i].type == TypeInt|| attributes[i].type == TypeReal)
				{
					memcpy(data+dataOffset, tuple+tupleOffset, sizeof(int));
					dataOffset += sizeof(int);
					tupleOffset += sizeof(int);
				}
				else
				{
					int length = 0;
					memcpy(&length, tuple+tupleOffset, sizeof(int));
					memcpy(data+dataOffset, &length, sizeof(int));
					dataOffset += sizeof(int);
					tupleOffset += sizeof(int);
					memcpy(data+dataOffset, tuple+tupleOffset, length);
					dataOffset += length;
					tupleOffset += length;
				}
				break;
			}
		}
	}
}

void Project::getMaxLengthOfTuple(int & length)
{
	for(unsigned i =0; i < attributes.size(); i++)
	{
		if(attributes[i].type == TypeInt|| attributes[i].type == TypeReal)
			length += attributes[i].length;
		else
			length += (attributes[i].length+sizeof(int));
	}
}

NLJoin::NLJoin(Iterator *leftIn,                             // Iterator of input R
               TableScan *rightIn,                           // TableScan Iterator of input S
               const Condition &condition,                   // Join condition
               const unsigned numPages                       // Number of pages can be used to do join (decided by the optimizer)
        ):iter(leftIn), scan(rightIn), cond(condition), numOfPages(numPages)
{
	leftIn->getAttributes(leftAttrs);
	rightIn->getAttributes(rightAttrs);
	addAttribute(leftAttrs);
	addAttribute(rightAttrs);
	setAttribute();
	cout<<"Loading...Please wait..."<<endl;
	setResults();
}

void NLJoin::clearanceJob()
{
	while(!results.empty())
	{
		char* trash = results.front();
		results.pop_front();
		delete[]trash;
	}
}

void NLJoin::addAttribute(vector<Attribute> &attributes)
{
	for(unsigned i = 0; i < attributes.size(); i++)
	{
		attrs.push_back(attributes[i]);
	}
}

void NLJoin::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	attrs = this->attrs;
}

RC NLJoin::getNextTuple(void* data)
{
	if( !results.empty() )
	{
		int length = resultLengths.front();
		char* tuple = results.front();
		memcpy(data, tuple, length);
		results.pop_front();
		resultLengths.pop_front();
		delete	[]tuple;
		return 0;
	}else
	{
		return QE_EOF;
	}
}

void NLJoin::setResults()
{
	int leftMaxLength = 0;
	getMaxLengthOfTuple(leftAttrs, leftMaxLength);
	int rightMaxLength = 0;
	getMaxLengthOfTuple(rightAttrs, rightMaxLength);
	char* leftTuple = new char[leftMaxLength];
	char* rightTuple = new char[rightMaxLength];
	while( iter->getNextTuple(leftTuple) == 0)
	{
		int leftAttributeLength = 0;
		int leftAttributeOffset = 0;
		getAttributeOffsetAndLength(leftAttrs, cond.lhsAttr, leftTuple, leftAttributeLength, leftAttributeOffset);
		char* leftAttribute = new char[leftAttributeLength];
		getAttribute(leftTuple, leftAttribute, leftAttributeOffset, leftAttributeLength);
		TableScan* rightScan = new TableScan(scan->rm, scan->tablename);
		while( rightScan->getNextTuple(rightTuple) == 0)
		{
			int rightAttributeLength = 0;
			int rightAttributeOffset = 0;
			getAttributeOffsetAndLength(rightAttrs, cond.rhsAttr, rightTuple, rightAttributeLength, rightAttributeOffset);
			char* rightAttribute = new char[rightAttributeLength];
			getAttribute(rightTuple, rightAttribute, rightAttributeOffset, rightAttributeLength);
			if( joinValues(leftAttribute, rightAttribute, leftAttributeLength, rightAttributeLength) )
			{
				char* result = new char[leftMaxLength+rightMaxLength];
				joinTuples(result, leftTuple, rightTuple);
				addResult(result);
			}
			delete[]rightAttribute;
		}
		delete rightScan;
		delete[]leftAttribute;
	}
	delete[]leftTuple;
	delete[]rightTuple;
	//delete[]result;

}

void NLJoin::addResult(char* tuple)
{
	results.push_back(tuple);
	//*****trace
	/*
	int test1,test2;
	memcpy(&test1, tuple+4, sizeof(int));
	memcpy(&test2, tuple+12, sizeof(int));
	char*test3 = results.front();
	int test4, test5;
	memcpy(&test4, test3+4, sizeof(int));
	memcpy(&test5, test3+12, sizeof(int));
	if (test4 != test5)
		cout<<"wrong";*/
}

void NLJoin::setAttribute()
{
	for(unsigned i = 0 ; i < leftAttrs.size(); i++)
	{
		if(strcmp(leftAttrs[i].name.c_str(), cond.lhsAttr.c_str()) == 0)
		{
			leftAttribute = leftAttrs[i];
			break;
		}
	}
}

void INLJoin::setAttribute()
{
	for(unsigned i = 0 ; i < leftAttrs.size(); i++)
	{
		if(strcmp(leftAttrs[i].name.c_str(), cond.lhsAttr.c_str()) == 0)
		{
			leftAttribute = leftAttrs[i];
			break;
		}
	}
}

bool NLJoin::joinValues(char* left, char* right, const int llength, const int rlength)
{
	if (llength != rlength)
		return false;
	if(leftAttribute.type == TypeInt || leftAttribute.type == TypeReal)
	{
		float leftF, rightF;
		memcpy(&leftF, left, sizeof(int));
		memcpy(&rightF, right, sizeof(int));
		switch(cond.op)
		{
		case 0:
			if(leftF == rightF)
				return true;
			break;
		case 1:
			if(leftF < rightF)
				return true;
			break;
		case 2:
			if(leftF > rightF)
				return true;
			break;
		case 3:
			if(leftF <= rightF)
				return true;
			break;
		case 4:
			if(leftF >= rightF)
				return true;
			break;
		case 5:
			if(leftF != rightF)
				return true;
			break;
		default:
			return true;
			break;
		}
	}else
	{
		switch(cond.op)
		{
		case 0:
			if(memcmp(left, right, llength) == 0)
				return true;
			break;
		case 1:
			if(memcmp(left, right, llength) < 0)
				return true;
			break;
		case 2:
			if(memcmp(left, right, llength) > 0)
				return true;
			break;
		case 3:
			if(memcmp(left, right, llength) <= 0)
				return true;
			break;
		case 4:
			if(memcmp(left, right, llength) >= 0)
				return true;
			break;
		case 5:
			if(memcmp(left, right, llength) != 0)
				return true;
			break;
		default:
			return true;
			break;
		}
	}
	return false;
}

void NLJoin::joinTuples(char* result, char* left, char* right)
{
	int tupleLength = 0;
	int rTupleLength = 0;
	//********trace
	/*
	int leftR = 0;
	int rightR = 0;
	memcpy(&leftR, left+4, sizeof(int));
	memcpy(&rightR, right, sizeof(int));*/
	getTupleLength(leftAttrs, left, tupleLength);
	getTupleLength(rightAttrs, right, rTupleLength);
	memcpy(result, left, tupleLength);
	memcpy(result+tupleLength, right, rTupleLength);
	int resultLength =  tupleLength+rTupleLength;
	addResultLength(resultLength);
}

void NLJoin::addResultLength(int length)
{
	resultLengths.push_back(length);
}

void NLJoin::getTupleLength(vector<Attribute>& attributes, char* tuple, int& result)
{
	int offset = 0;
	for(unsigned i = 0; i < attributes.size(); i++)
	{
		if(attributes[i].type == TypeInt|| attributes[i].type == TypeReal)
		{
			result += sizeof(int);
			offset += sizeof(int);
		}
		else
		{
			int length;
			memcpy(&length, tuple+offset, sizeof(int));
			result+= (sizeof(int)+length);
			offset += (sizeof(int)+length);
		}
	}
}

void NLJoin::getAttribute(char* tuple, char* attribute, const int offset, const int lengthOfData)
{
	if(leftAttribute.type == TypeInt || leftAttribute.type == TypeReal)
	{
		memcpy(attribute, tuple+offset, sizeof(int));
	}else
	{
		memcpy(attribute, tuple+offset+sizeof(int), lengthOfData);
	}
}

void NLJoin::getAttributeOffsetAndLength(vector<Attribute> & attributes, string& name, char* tuple, int& length, int &offset)
{
	for(unsigned i = 0; i < attributes.size(); i++)
	{
		if( strcmp(attributes[i].name.c_str(), name.c_str() ) == 0){
			if( attributes[i].type == TypeInt || attributes[i].type == TypeReal )
				length = sizeof(int);
			else
				memcpy(&length,(char*) tuple+offset, sizeof(int));
			break;
		}else
		{
			if( attributes[i].type == TypeInt || attributes[i].type == TypeReal ){
				offset += sizeof(int);
			}
			else{
				int stringLength = 0;
				memcpy(&stringLength, tuple+offset, sizeof(int));
				offset += (stringLength+sizeof(int));
			}
		}
	}
}

void NLJoin::getMaxLengthOfTuple(vector<Attribute>& attributes, int & length)
{
	for(unsigned i =0; i < attributes.size(); i++)
	{
		if(attributes[i].type == TypeInt|| attributes[i].type == TypeReal)
			length += attributes[i].length;
		else
			length += (attributes[i].length+sizeof(int));
	}
}

INLJoin::INLJoin(Iterator *leftIn,                               // Iterator of input R
         IndexScan *rightIn,                             // IndexScan Iterator of input S
         const Condition &condition,                     // Join condition
         const unsigned numPages                         // Number of pages can be used to do join (decided by the optimizer)
 ):iter(leftIn), scan(rightIn), cond(condition), numOfPages(numPages)
{
	leftIn->getAttributes(leftAttrs);
	rightIn->getAttributes(rightAttrs);
	addAttribute(leftAttrs);
	addAttribute(rightAttrs);
	cout<<"Loading...Please wait..."<<endl;
	setAttribute();
	setResults();
}

void INLJoin::clearanceJob()
{
	while(!results.empty())
	{
		char* trash = results.front();
		results.pop_front();
		delete[]trash;
	}
}
void INLJoin::addAttribute(vector<Attribute> &attributes)
{
	for(unsigned i = 0; i < attributes.size(); i++)
	{
		attrs.push_back(attributes[i]);
	}
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	attrs = this->attrs;
}

RC INLJoin::getNextTuple(void* data)
{
	if( !results.empty() )
	{
		int length = resultLengths.front();
		char* tuple = results.front();
		memcpy(data, tuple, length);
		results.pop_front();
		resultLengths.pop_front();
		delete	[]tuple;
		return 0;
	}else
	{
		return QE_EOF;
	}
}

void INLJoin::setResults()
{
	int leftMaxLength = 0;
	getMaxLengthOfTuple(leftAttrs, leftMaxLength);
	int rightMaxLength = 0;
	getMaxLengthOfTuple(rightAttrs, rightMaxLength);
	char* leftTuple = new char[leftMaxLength];
	char* rightTuple = new char[rightMaxLength];
	while( iter->getNextTuple(leftTuple) == 0)
	{
		int leftAttributeLength = 0;
		int leftAttributeOffset = 0;
		getAttributeOffsetAndLength(leftAttrs, cond.lhsAttr, leftTuple, leftAttributeLength, leftAttributeOffset);
		char* leftAttribute = new char[leftAttributeLength];
		getAttribute(leftTuple, leftAttribute, leftAttributeOffset, leftAttributeLength);
		TableScan* rightScan = new TableScan(scan->rm, scan->tablename);
		while( rightScan->getNextTuple(rightTuple) == 0)
		{
			int rightAttributeLength = 0;
			int rightAttributeOffset = 0;
			getAttributeOffsetAndLength(rightAttrs, cond.rhsAttr, rightTuple, rightAttributeLength, rightAttributeOffset);
			char* rightAttribute = new char[rightAttributeLength];
			getAttribute(rightTuple, rightAttribute, rightAttributeOffset, rightAttributeLength);
			if( joinValues(leftAttribute, rightAttribute, leftAttributeLength, rightAttributeLength) )
			{
				char* result = new char[leftMaxLength+rightMaxLength];
				joinTuples(result, leftTuple, rightTuple);
				addResult(result);
			}
			delete[]rightAttribute;
		}
		delete rightScan;
		delete[]leftAttribute;
	}
	delete[]leftTuple;
	delete[]rightTuple;
	//delete[]result;

}

void INLJoin::addResult(char* tuple)
{
	results.push_back(tuple);
}

bool INLJoin::joinValues(char* left, char* right, const int llength, const int rlength)
{
	if (llength != rlength)
		return false;
	if(leftAttribute.type == TypeInt || leftAttribute.type == TypeReal)
	{
		float leftF, rightF;
		memcpy(&leftF, left, sizeof(int));
		memcpy(&rightF, right, sizeof(int));
		switch(cond.op)
		{
		case 0:
			if(leftF == rightF)
				return true;
			break;
		case 1:
			if(leftF < rightF)
				return true;
			break;
		case 2:
			if(leftF > rightF)
				return true;
			break;
		case 3:
			if(leftF <= rightF)
				return true;
			break;
		case 4:
			if(leftF >= rightF)
				return true;
			break;
		case 5:
			if(leftF != rightF)
				return true;
			break;
		default:
			return true;
			break;
		}
	}else
	{
		switch(cond.op)
		{
		case 0:
			if(memcmp(left, right, llength) == 0)
				return true;
			break;
		case 1:
			if(memcmp(left, right, llength) < 0)
				return true;
			break;
		case 2:
			if(memcmp(left, right, llength) > 0)
				return true;
			break;
		case 3:
			if(memcmp(left, right, llength) <= 0)
				return true;
			break;
		case 4:
			if(memcmp(left, right, llength) >= 0)
				return true;
			break;
		case 5:
			if(memcmp(left, right, llength) != 0)
				return true;
			break;
		default:
			return true;
			break;
		}
	}
	return false;
}

void INLJoin::joinTuples(char* result, char* left, char* right)
{
	int tupleLength = 0;
	int rTupleLength = 0;
	//********trace
	/*
	int leftR = 0;
	int rightR = 0;
	memcpy(&leftR, left+4, sizeof(int));
	memcpy(&rightR, right, sizeof(int));*/
	getTupleLength(leftAttrs, left, tupleLength);
	getTupleLength(rightAttrs, right, rTupleLength);
	memcpy(result, left, tupleLength);
	memcpy(result+tupleLength, right, rTupleLength);
	int resultLength =  tupleLength+rTupleLength;
	addResultLength(resultLength);
}

void INLJoin::addResultLength(int length)
{
	resultLengths.push_back(length);
}

void INLJoin::getTupleLength(vector<Attribute>& attributes, char* tuple, int& result)
{
	int offset = 0;
	for(unsigned i = 0; i < attributes.size(); i++)
	{
		if(attributes[i].type == TypeInt|| attributes[i].type == TypeReal)
		{
			result += sizeof(int);
			offset += sizeof(int);
		}
		else
		{
			int length;
			memcpy(&length, tuple+offset, sizeof(int));
			result+= (sizeof(int)+length);
			offset += (sizeof(int)+length);
		}
	}
}

void INLJoin::getAttribute(char* tuple, char* attribute, const int offset, const int lengthOfData)
{
	if(leftAttribute.type == TypeInt || leftAttribute.type == TypeReal)
	{
		memcpy(attribute, tuple+offset, sizeof(int));
	}else
	{
		memcpy(attribute, tuple+offset+sizeof(int), lengthOfData);
	}
}

void INLJoin::getAttributeOffsetAndLength(vector<Attribute> & attributes, string& name, char* tuple, int& length, int &offset)
{
	for(unsigned i = 0; i < attributes.size(); i++)
	{
		if( strcmp(attributes[i].name.c_str(), name.c_str() ) == 0){
			if( attributes[i].type == TypeInt || attributes[i].type == TypeReal )
				length = sizeof(int);
			else
				memcpy(&length,(char*) tuple+offset, sizeof(int));
			break;
		}else
		{
			if( attributes[i].type == TypeInt || attributes[i].type == TypeReal ){
				offset += sizeof(int);
			}
			else{
				int stringLength = 0;
				memcpy(&stringLength, tuple+offset, sizeof(int));
				offset += (stringLength+sizeof(int));
			}
		}
	}
}

void INLJoin::getMaxLengthOfTuple(vector<Attribute>& attributes, int & length)
{
	for(unsigned i =0; i < attributes.size(); i++)
	{
		if(attributes[i].type == TypeInt|| attributes[i].type == TypeReal)
			length += attributes[i].length;
		else
			length += (attributes[i].length+sizeof(int));
	}
}

HashJoin::HashJoin(Iterator *leftIn,                                // Iterator of input R
         Iterator *rightIn,                               // Iterator of input S
         const Condition &condition,                      // Join condition
         const unsigned numPages                          // Number of pages can be used to do join (decided by the optimizer)
):leftIter(leftIn), rightIter(rightIn), cond(condition), numOfPages(numPages)
{
	leftIn->getAttributes(leftAttrs);
	rightIn->getAttributes(rightAttrs);
	addAttribute(leftAttrs);
	addAttribute(rightAttrs);
	setAttribute();
	leftPartitionName = "l_temp";
	rightPartitionName = "r_temp";
	createPartitions(numOfPages-1);
	numberOfPartitions = numOfPages-1;
	hashPartition(leftIter, numberOfPartitions);
	hashPartition(rightIter, numberOfPartitions);
	buildIn();
	deletePartitions(numOfPages-1);
}

void HashJoin::addAttribute(vector<Attribute> &attributes)
{
	for(unsigned i = 0; i < attributes.size(); i++)
	{
		attrs.push_back(attributes[i]);
	}
}

void HashJoin::setAttribute()
{
	for(unsigned i = 0 ; i < rightAttrs.size(); i++)
	{
		if(strcmp(rightAttrs[i].name.c_str(), cond.rhsAttr.c_str()) == 0)
		{
			attribute = rightAttrs[i];
			break;
		}
	}
}

void HashJoin::getAttributes(vector<Attribute>& attrs)const
{
	attrs.clear();
	attrs = this->attrs;
}

RC HashJoin::getNextTuple(void* data)
{
	if(results.empty())
	{
		return IX_EOF;
	}
	char* result = results.front();
	int length = resultLengths.front();
	memcpy(data,result, length);
	results.pop_front();
	resultLengths.pop_front();
	delete[]result;
	return 0;
}

void HashJoin::buildIn()
{
	string leftName = leftPartitionName;
	for(int i =0 ; i < numberOfPartitions ; i++)
	{
		string rightName = rightPartitionName;
		leftName += "t";
		readLeftPartition(leftName);
		for(int j = 0; j < numberOfPartitions ; j++)
		{
			rightName +="t";
			matchRightPartition(rightName);
			setResult();
			clearRight();
		}
		clearLeft();
	}
}

void HashJoin::clearLeft()
{
	while(! hashTable.empty())
	{
		char* trash = hashTable.back();
		hashTable.pop_back();
		delete[]trash;
	}
	lengths.clear();
}

void HashJoin::clearRight()
{
	while(! innerTable.empty())
	{
		char* trash = innerTable.back();
		innerTable.pop_back();
		delete[]trash;
	}
	innerLengths.clear();
}

void HashJoin::setResult()
{
	for(unsigned j = 0; j < hashTable.size(); j++)
	{
		char* leftRecord = hashTable[j];
		int lLength = lengths[j];
		char* leftAttribute = new char[lLength];
		int lAttributeLength;
		getAttribute(leftAttrs, cond.lhsAttr, leftRecord, leftAttribute, lAttributeLength);
		for(unsigned i = 0; i < innerTable.size(); i++)
		{
			char* rightRecord = innerTable[i];
			int rLength = innerLengths[i];
			char* rightAttribute = new char[rLength];
			int rAttributeLength;

			getAttribute(rightAttrs, cond.rhsAttr, rightRecord, rightAttribute, rAttributeLength);
			if( joinValues(leftAttribute, rightAttribute , lAttributeLength, rAttributeLength) )
			{
				lLength = 0;
				rLength = 0;
				getTupleLength(leftAttrs, leftRecord, lLength);
				getTupleLength(rightAttrs, rightRecord, rLength);
				joinTuples(leftRecord, rightRecord, lLength, rLength);
			}
			delete[]rightAttribute;
		}
		delete[]leftAttribute;
	}
}

void HashJoin::getAttribute(vector<Attribute> & attrs, string& name,char* record, char* attr, int& attributeLength)
{
	int length = 0;
	int offset = 0;

	getAttributeOffsetAndLength(attrs,  name, record, length, offset );
	memcpy(attr, record+offset, length);
	attributeLength = length;
}

void HashJoin::joinTuples(char* left, char* right, int lTupleLength, int rTupleLength)
{
	int resultLength =  lTupleLength+rTupleLength;
	char* result = new char[resultLength];
	memcpy(result, left, lTupleLength);
	memcpy(result+lTupleLength, right, rTupleLength);
	addResult(result, resultLength);
}

void HashJoin::getTupleLength(vector<Attribute>&attrs, char* record, int &length)
{
	for(unsigned i = 0; i < attrs.size(); i++)
	{
		if(attrs[i].type == TypeInt || attrs[i].type ==TypeReal)
		{
			length+=sizeof(int);
		}else{
			int temp;
			memcpy(&temp, record+length, sizeof(int) );
			length += temp;
			length += sizeof(int);
		}
	}
}

void HashJoin::addResult(char* result, int resultLength)
{
	results.push_back(result);
	resultLengths.push_back(resultLength);
}

bool HashJoin::joinValues(char* left, char* right, const int llength, const int rlength)
{
	if (llength != rlength)
		return false;
	if(attribute.type == TypeInt || attribute.type == TypeReal)
	{
		float leftF, rightF;
		memcpy(&leftF, left, sizeof(int));
		memcpy(&rightF, right, sizeof(int));
		switch(cond.op)
		{
		case 0:
			if(leftF == rightF)
				return true;
			break;
		case 1:
			if(leftF < rightF)
				return true;
			break;
		case 2:
			if(leftF > rightF)
				return true;
			break;
		case 3:
			if(leftF <= rightF)
				return true;
			break;
		case 4:
			if(leftF >= rightF)
				return true;
			break;
		case 5:
			if(leftF != rightF)
				return true;
			break;
		default:
			return true;
			break;
		}
	}else
	{
		switch(cond.op)
		{
		case 0:
			if(memcmp(left, right, llength) == 0)
				return true;
			break;
		case 1:
			if(memcmp(left, right, llength) < 0)
				return true;
			break;
		case 2:
			if(memcmp(left, right, llength) > 0)
				return true;
			break;
		case 3:
			if(memcmp(left, right, llength) <= 0)
				return true;
			break;
		case 4:
			if(memcmp(left, right, llength) >= 0)
				return true;
			break;
		case 5:
			if(memcmp(left, right, llength) != 0)
				return true;
			break;
		default:
			return true;
			break;
		}
	}
	return false;
}


void HashJoin::matchRightPartition(string name)
{
	PF_FileHandle handle;
	pf->OpenFile(name.c_str(), handle);
	for(unsigned i = 0; i < handle.GetNumberOfPages(); i++)
	{
		char* page = new char[4096];
		handle.ReadPage(i, page);
		int recordNumber = 0;
		memcpy(&recordNumber, page, sizeof(int));
		int offset = 4;
		for(int j =0; j < recordNumber; j++)
		{
			int currentLength = 0;
			memcpy(&currentLength, page+offset, sizeof(int));
			offset += sizeof(int);
			char* record = new char[currentLength];
			memcpy(record, page+offset, currentLength);
			innerTable.push_back(record);
			innerLengths.push_back(currentLength);
			offset += currentLength;
		}
		delete[]page;
	}
	pf->CloseFile(handle);
}

void HashJoin::readLeftPartition(string name)
{
	PF_FileHandle handle;
	pf->OpenFile(name.c_str(), handle);
	for(unsigned i = 0; i < handle.GetNumberOfPages(); i++)
	{
		char* page = new char[4096];
		handle.ReadPage(i, page);
		int recordNumber = 0;
		memcpy(&recordNumber, page, sizeof(int));
		int offset = 4;
		for(int j =0; j < recordNumber; j++)
		{
			int currentLength = 0;
			memcpy(&currentLength, page+offset, sizeof(int));
			offset += sizeof(int);
			char* record = new char[currentLength];
			memcpy(record, page+offset, currentLength);
			buildInMemoryHash(record, currentLength);
			offset+=currentLength;
		}
		delete[]page;
	}
	pf->CloseFile(handle);
}

void HashJoin::buildInMemoryHash(char* record, int length)
{
	hashTable.push_back(record);
	lengths.push_back(length);
}

void HashJoin::hashValue(char* record, int& v)
{
	int size = 100;
	string attributeName = cond.lhsAttr;
	int attributeLength = 0;
	int attributeOffset = 0;
	getAttributeOffsetAndLength(attrs, attributeName, record, attributeLength, attributeOffset);
	char* va = new char[attributeLength];
	memcpy(va, record+attributeOffset, attributeLength);
	if(attribute.type == TypeInt || attribute.type == TypeReal)
	{
		float value;
		memcpy(&value, va , sizeof(int));
		for(int i = 0; i < size; i++)
		{
			if((int)value%(size) == i)
			{
				v = i;
			}
		}
	}else {
		v = rand()%(size);
	}
	delete[]va;
}

void HashJoin::hashPartition(Iterator * iter, int size)
{
	int length = 0;
	vector<Attribute> attrs;
	iter->getAttributes(attrs);
	getMaxLengthOfTuple(attrs, length);
	char* record = new char[length];
	while(iter->getNextTuple(record) == 0)
	{
		hashRecord(iter, record, size, length);
	}
	delete[]record;
}

void HashJoin::hashRecord(Iterator* iter, char* record, int size, int length)
{
	string leftTableName = leftPartitionName;
	string rightTableName = rightPartitionName;
	string attributeName;
	int attributeLength = 0;
	int attributeOffset = 0;
	if(iter == leftIter){
		attributeName = cond.lhsAttr;
		getAttributeOffsetAndLength(leftAttrs, attributeName, record, attributeLength, attributeOffset);
	}
	else if(iter == rightIter){
		attributeName = cond.rhsAttr;
		getAttributeOffsetAndLength(rightAttrs, attributeName, record, attributeLength, attributeOffset);
	}
	//****trace
	int test1;
	memcpy(&test1, record+4, sizeof(int));

	char* v = new char[attributeLength];
	memcpy(v, record+attributeOffset, attributeLength);
	if(attribute.type == TypeInt || attribute.type == TypeReal)
	{
		float value;
		memcpy(&value, v , sizeof(int));
		for(int i = 0; i < size; i++)
		{
			leftTableName += "t";
			rightTableName += "t";
			if((int)value%(size) == i)
			{
				if(iter == leftIter)
				{
					putRecordInFile(leftTableName, record, length);
					break;
				}else{
					putRecordInFile(rightTableName, record, length);
					break;
				}
			}
		}
	}else {
		int i = rand()%(size);
		for(int j = 0; j <=i; j++)
		{
			leftTableName += "t";
			rightTableName += "t";
		}
		if(iter == leftIter)
		{
			putRecordInFile(leftTableName, record, length);
		}else{
			putRecordInFile(rightTableName, record, length);
		}
	}
	delete[]v;
}

void HashJoin::putRecordInFile(string name, char* record, int length)
{
	PF_FileHandle handle;
	pf->OpenFile(name.c_str(), handle);
	for(unsigned  i =0; i < handle.GetNumberOfPages(); i++)
	{
		char* page = new char[4096];
		handle.ReadPage(i, page);
		int freespace = 0;
		int recordNumber = 0;
		memcpy(&recordNumber, page, sizeof(int));
		memcpy(&freespace, page+4096-4, sizeof(int));
		if(length+4<freespace)
		{
			int offset = 4;
			for(int j = 0; j < recordNumber; j++)
			{
				int currentLength = 0;
				memcpy(&currentLength, page+offset, sizeof(int));
				offset+=(currentLength+sizeof(int));
			}
			memcpy(page+offset, &length, sizeof(int));	//put length into file
			offset += sizeof(int);
			memcpy(page+offset, record, length);	//put record into file
			recordNumber++;
			memcpy(page, &recordNumber, sizeof(int));	//update record number
			freespace -= (length+sizeof(int));
			memcpy(page+4096-4, &freespace, sizeof(int));	//update freespace
		}else
		{
			if( (i+1) == handle.GetNumberOfPages() )
			{
				char* firstPage = new char[4096];
				int numberOfRecords = 0;
				int freespace = 4096-8;
				memcpy(firstPage, &numberOfRecords, sizeof(int));
				memcpy(firstPage+4096-4, &freespace, sizeof(int));
				handle.AppendPage(firstPage);
				delete[]firstPage;
			}
		}
		handle.WritePage(i, page);
		delete[]page;
	}
	pf->CloseFile(handle);
}

void HashJoin::createPartitions(int number)
{
	string leftTableName = leftPartitionName;
	string rightTableName = rightPartitionName;
	for(int i = 0; i < number; i++)
	{
		leftTableName += "t";
		rightTableName += "t";
		pf->CreateFile(leftTableName.c_str());
		pf->CreateFile(rightTableName.c_str());
		initializePartition(leftTableName);
		initializePartition(rightTableName);

	}
}

void HashJoin::initializePartition(string name)
{
	PF_FileHandle handle;
	pf->OpenFile(name.c_str(), handle);
	char* firstPage = new char[4096];
	int numberOfRecords = 0;
	int freespace = 4096-8;
	memcpy(firstPage, &numberOfRecords, sizeof(int));
	memcpy(firstPage+4096-4, &freespace, sizeof(int));
	handle.AppendPage(firstPage);
	delete[]firstPage;
	pf->CloseFile(handle);
}

void HashJoin::deletePartitions(int number)
{
	string leftTableName = leftPartitionName;
	string rightTableName = rightPartitionName;
	for(int i = 0; i < number; i++)
	{
		leftTableName += "t";
		rightTableName += "t";
		pf->DestroyFile(leftTableName.c_str());
		pf->DestroyFile(rightTableName.c_str());
	}
}


void HashJoin::getAttributeOffsetAndLength(vector<Attribute> & attributes, string& name, char* tuple, int& length, int &offset)
{
	for(unsigned i = 0; i < attributes.size(); i++)
	{
		if( strcmp(attributes[i].name.c_str(), name.c_str() ) == 0){
			if( attributes[i].type == TypeInt || attributes[i].type == TypeReal )
				length = sizeof(int);
			else
				{
				memcpy(&length,(char*) tuple+offset, sizeof(int));
				offset+=sizeof(int);
				}
			break;
		}else
		{
			if( attributes[i].type == TypeInt || attributes[i].type == TypeReal ){
				offset += sizeof(int);
			}
			else{
				int stringLength = 0;
				memcpy(&stringLength, tuple+offset, sizeof(int));
				offset += (stringLength+sizeof(int));
			}
		}
	}
}

void HashJoin::getMaxLengthOfTuple(vector<Attribute>& attributes, int & length)
{
	for(unsigned i =0; i < attributes.size(); i++)
	{
		if(attributes[i].type == TypeInt|| attributes[i].type == TypeReal)
			length += attributes[i].length;
		else
			length += (attributes[i].length+sizeof(int));
	}
}

void HashJoin::clearanceJob()
{
	while(!hashTable.empty())
	{
		char* trash = hashTable.back();
		hashTable.pop_back();
		delete[]trash;
	}
	while(!innerTable.empty())
	{
		char* trash = innerTable.back();
		innerTable.pop_back();
		delete[]trash;
	}
	while(!results.empty())
	{
		char*trash = results.front();
		results.pop_front();
		delete[]trash;
	}
}

Aggregate::Aggregate(Iterator *input,                              // Iterator of input R
                  Attribute aggAttr,                            // The attribute over which we are computing an aggregate
                  AggregateOp op):iter(input), aggregateAttribute(aggAttr), operation(op)
{
	button = 0;
	flag = 0;
	input->getAttributes(attributes);
	prepareResult();
}

void Aggregate::prepareResult()
{
	int maxLengthOfTuple = 0;
	getMaxLengthOfTuple(maxLengthOfTuple);
	char* tuple = new char[maxLengthOfTuple];
	while(iter->getNextTuple(tuple) == 0)
	{
		int attributeLength = 0;
		int attributeOffset = 0;
		getAttributeOffsetAndLength(attributes, aggregateAttribute.name, tuple, attributeLength, attributeOffset);
		char* attribute = new char[attributeLength];
		memcpy(attribute, tuple+attributeOffset, attributeLength);
		addAttribute(attribute);
		delete[]attribute;
	}
	delete[]tuple;
}

void Aggregate::addAttribute(char* data)
{
	if(aggregateAttribute.type == TypeInt)
	{
		int value;
		memcpy(&value, data, sizeof(int));
		results.push_back(value);
	}else if(aggregateAttribute.type == TypeReal){
		float value;
		memcpy(&value, data, sizeof(int));
		results.push_back(value);
	}
}

void Aggregate:: getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	attrs = this->attributes;

    unsigned i;
    // Please name the output attribute as aggregateOp(aggAttr)
     // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
     // output attrname = "MAX(rel.attr)"
    for(i = 0; i < attrs.size(); ++i)
    {
    	string temp;
    	getOperation(temp);
    	temp += "(";
    	temp += attributes[i].name;
    	temp += ")";
    	attrs[i].name = temp;
    }
}

void Aggregate::getOperation(string& name) const
{
	switch(operation)
	{
	case 0: name = "MIN";break;
	case 1: name = "MAX";break;
	case 2: name = "SUM";break;
	case 3: name = "AVG";break;
	case 4: name = "COUNT"; break;
	}
}

void Aggregate::getMaxLengthOfTuple(int & length)
{
	for(unsigned i =0; i < attributes.size(); i++)
	{
		if(attributes[i].type == TypeInt|| attributes[i].type == TypeReal)
			length += attributes[i].length;
		else
			length += (attributes[i].length+sizeof(int));
	}
}

void Aggregate::getAttributeOffsetAndLength(vector<Attribute>&attrs, string& name,  void* data, int &length, int& offset)
{
		for(unsigned i = 0; i < attrs.size(); i++)
		{
			if( strcmp(attrs[i].name.c_str(), name.c_str() ) == 0){
				if( attrs[i].type == TypeInt || attrs[i].type == TypeReal )
					length = sizeof(int);
				else
					memcpy(&length,(char*) data+offset, sizeof(int));
				break;
			}
			else{
				if( attrs[i].type == TypeInt || attrs[i].type == TypeReal ){
					offset += sizeof(int);
				}
				else{
					int stringLength = 0;
					memcpy(&stringLength,(char*)data+offset, sizeof(int));
					offset += (stringLength+sizeof(int));
				}
			}
		}
}

RC Aggregate::getNextTuple(void *data)
{
	if(button == 0){
		if(flag == 1)
		{
			flag = 0;
			return QE_EOF;
		}
		getResult(data);
		flag ++;
	}
	else if(button == 1)
	{
		if( it == maps.end() || it == counts.end() )
			return QE_EOF;
		char* group = it->first;
		int groupLength = lengths.find(group)->second;
		memcpy(data, group, groupLength);
		memcpy(data+groupLength, &it->second, sizeof(int));
		it++;
		/*
		switch(operation)
		{
		case 0:
		case 1:
		case 2:
		case 3: {

			break;
		}
		case 4:  {
			char* group = it->first;
			int groupLength = lengths.find(group)->second;
			memcpy
			break;
		}
		}
*/
	}
	return 0;
}

void Aggregate::getResult(void* data)
{
	switch(operation)
	{
	case 0: {
		sort(results.begin(), results.end());
		float minV = results.front();
		memcpy(data, &minV, sizeof(int));
		break;
	}
	case 1: {
		sort(results.begin(), results.end());
		float maxV = results.back();
		memcpy(data, &maxV, sizeof(int));
		break;
	}
	case 2: {
		float sumV = sum();
		memcpy(data, &sumV, sizeof(int));
		break;
	}
	case 3: {
		float averageV = average();
		memcpy(data,&averageV, sizeof(int));
		break;
	}
	case 4: {
		float countV = count();
		memcpy(data,&countV, sizeof(int));
		break;
	}
	}
}

float Aggregate::sum()
{
	float sum = 0;
	for(unsigned i = 0; i < results.size(); i++)
	{
		sum+=results[i];
	}
	return sum;
}

float Aggregate::average()
{
	float sumV = sum();
	float average = 0;
	average = sumV/results.size();
	return average;
}

float Aggregate::count()
{
	return (float)results.size();
}

 Aggregate::Aggregate(Iterator *input,                              // Iterator of input R
        Attribute aggAttr,                            // The attribute over which we are computing an aggregate
        Attribute gAttr,                              // The attribute over which we are grouping the tuples
        AggregateOp op                                // Aggregate operation
):iter(input), aggregateAttribute(aggAttr),groupAttribute(gAttr), operation(op)
{
	button = 1;
	flag = 0;
	input->getAttributes(attributes);
	prepareResult2();
	setIterator();
}

 void Aggregate::setIterator()
 {
	 switch(operation)
	 {
	 case 0:
	 case 1:
	 case 2:
	 case 3: it = maps.begin(); break;
	 case 4: it = counts.begin(); break;
	 }
 }

void Aggregate::prepareResult2()
{
	int maxLengthOfTuple = 0;
	getMaxLengthOfTuple(maxLengthOfTuple);
	char* tuple = new char[maxLengthOfTuple];
	while(iter->getNextTuple(tuple) == 0)
	{
		prepareAttribute(tuple);
	}
	delete[]tuple;
}

void Aggregate::prepareAttribute(char* record)
{
	int attributeLength = 0;
	int attributeOffset = 0;
	int groupLength = 0;
	int groupOffset = 0;
	getAttributeOffsetAndLength(attributes, aggregateAttribute.name,  record,attributeLength, attributeOffset);
	getAttributeOffsetAndLength(attributes, groupAttribute.name, record, groupLength, groupOffset );
	char* attribute = new char[attributeLength];
	char* groupAttribute = new char[groupLength];
	memcpy(attribute, record+attributeOffset, attributeLength);
	memcpy(groupAttribute, record+ groupOffset, groupLength);
	if(lengths.find(groupAttribute) == lengths.end())
		lengths.insert(pair<char*, int>(groupAttribute, groupLength));
	setMap(attribute, groupAttribute);

	delete[]attribute;
	//delete[]groupAttribute;
}

void Aggregate::setMap(char* value, char* key)
{
	if( maps.find(key) == maps.end() )		//no key in map
	{
		if(aggregateAttribute.type == TypeInt){
			int v;
			memcpy(&v, value, sizeof(int));
			maps.insert( pair<char*,float>(key, v ) );
		}else if(aggregateAttribute.type == TypeReal){
			float v;
			memcpy(&v, value, sizeof(int));
			maps.insert( pair<char*, float>(key, v) );
		}
		counts.insert( pair<char*, int>(key, 1));
	}else{
		counts.find(key)->second++;
		if(aggregateAttribute.type == TypeInt)
		{
			int v;
			memcpy(&v, value, sizeof(int));
			switch(operation){
			case 0: {
				if(v < maps.find(key)->second)
					maps.find(key)->second = v;
				break;
			}
			case 1: {
				if(v > maps.find(key)->second)
					maps.find(key)->second = v;
				break;
			}
			case 2: {
				maps.find(key)->second += v;
				break;
			}
			case 3: {
				float temp = maps.find(key)->second*(counts.find(key)->second-1) + v;
				temp /= counts.find(key)->second;
				maps.find(key)->second = temp;
				break;
			}
			default:break;
			}

		}else if(aggregateAttribute.type == TypeReal)
		{
			float v;
			memcpy(&v,value,sizeof(int));
			switch(operation){
			case 0: {
				if(v < maps.find(key)->second)
					maps.find(key)->second = v;
				break;
			}
			case 1: {
				if(v > maps.find(key)->second)
					maps.find(key)->second = v;
				break;
			}
			case 2: {
				maps.find(key)->second += v;
				break;
			}
			case 3: {
				float temp = maps.find(key)->second*(counts.find(key)->second-1) + v;
				temp /= counts.find(key)->second;
				maps.find(key)->second = temp;
				break;
			}
			default:break;
			}
		}
	}
}

void Aggregate::clearanceJob()
{
	for(it = maps.begin(); it!=maps.end();it++)
	{
		char* trash = it->first;
		delete[]trash;
	}
	results.clear();
	maps.clear();
	counts.clear();
	lengths.clear();
}


















