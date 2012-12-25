
#include "rm.h"
#include <stdio.h>
#include <string.h>
#include <cstdlib>

RM* RM::_rm = 0;

RM* RM::Instance()
{
    if(!_rm)
        _rm = new RM();
    
    return _rm;
}

RM::RM()
{
	pf = PF_Manager::Instance();
	createCatalog();
}

RM::~RM()
{
}

/*
 * precondition:
 * postcondition: Create file, open file, make pair tableName, attrs in cache, close
 * 1. create file correspond to the table
 *
 * 2.** insert table info into catalog file
 */
RC RM::createTable(const string tableName, const vector<Attribute> &attrs)
{
 	PF_FileHandle tableHandle;

	if( pf->CreateFile( tableName.c_str() )==0)
		insertTableInfo(tableName, attrs);

	return 0;
}

/*
 * Precondition: table exists
 * Postcondition: remove file
 */
RC RM::deleteTable(const string tableName)
{
	if(pf->DestroyFile(tableName.c_str()) == 0)
		deleteTableInfo(tableName);
	return 0;
}

/*
 * Precondition: return -1 if table not found
 * Postcondition: get attrs
 */
  RC RM::getAttributes(const string tableName, vector<Attribute> &attrs)
  {
	  PF_FileHandle catalogHandle;
	  pf->OpenFile("Catalog", catalogHandle);
	  char * page = new char[4096];
	  for(unsigned i = 1; i < catalogHandle.GetNumberOfPages(); i++)
	  {
		  catalogHandle.ReadPage(i, page);
		  int currentSlotNumber = 0;
		  memcpy(&currentSlotNumber, page+4096-8+4, sizeof(int));
		  for(int j = 1; j < currentSlotNumber+1; j++)
		  {
			  int lengthOfRecord = 0;
			  memcpy(&lengthOfRecord, page + 4096-8*(j+1) + 4, sizeof(int));
			  if(lengthOfRecord > 0)
			  {
				  char* record = new char[lengthOfRecord];
				  int recordOffset = 0;
				  memcpy(&recordOffset, page + 4096-8*j, sizeof(int));
				  memcpy(record, page+recordOffset, lengthOfRecord);
				  int recordTableNameLength = 0;
				  memcpy(&recordTableNameLength, record, sizeof(int));
				  if(recordTableNameLength == tableName.length())
				  {
					  char* recordTableName =  new char[tableName.length()];
					  memcpy(recordTableName, record+4, tableName.length());

					  if(memcmp(recordTableName, tableName.c_str(), tableName.length()) == 0)
					  {
						  Attribute attribute;
						  int offset = 0;
						  offset += sizeof(int) + tableName.length() ;
						  int attributeNameLength =0;
						  memcpy(&attributeNameLength, record+offset, sizeof(int));
						  offset += sizeof(int);

						  char* name = new char[attributeNameLength+1];
						  memcpy(name, record+offset, attributeNameLength);
						  name[attributeNameLength] = '\0';
						  attribute.name = name;
						  attribute.name.substr(0, attributeNameLength);

						 // memcpy(attribute.name.c_str(), record+offset, attributeNameLength);

						  offset += attributeNameLength;
						  memcpy(&attribute.length, record+offset, sizeof(int));
						  offset += sizeof(int);
						  memcpy(&attribute.type, record+offset, sizeof(int));

						  attrs.push_back(attribute);
						 delete[]name;
					  }
					  delete [] recordTableName;
				  }
				  delete [] record;
			  }
		  }
	  }
	  pf->CloseFile(catalogHandle);
	  delete[]page;

	  return 0;
  }

  /*
   * Precondition: RM is called
   * Postcondition: create a catalog for all table information
   * 1. create a separate file naming "Catalog" if catalog not exists; open catalog is exists
   * 2. append a page into catalog
   * 3. close catalog file
   */
RC RM::createCatalog()
{
	pf->CreateFile("Catalog");
	/*
	pf->OpenFile("Catalog", catalogHandle);
	char page = new char[4096];
	catalogHandle.AppendPage(page);
	pf->CloseFile(catalogHandle);
	delete[]page;
*/
	return 0;
}

/*
 * Precondition:
 * Postcondition: write table info into catalog
 * format:
 *  flag(table delete? negative== delete)|table name|attribute name|attribute length|attribute type
 * 1. open catalog. using fixed-length records
 * 2. read page info into memory
 * 3. write table info into to file
 */
RC RM::insertTableInfo(const string tableName, const vector<Attribute> &attrs)
{
	PF_FileHandle catalogHandle;
	int directoryNum = 0;
	pf->OpenFile("Catalog", catalogHandle);
	char *page = new char[4096];
	if(catalogHandle.GetNumberOfPages() == 0)
	{
		prepareNewDirectory(page);
		catalogHandle.AppendPage(page);
	}
	//trace
	int a;
	memcpy(&a, page, sizeof(int));
	//prepare record in catalog
	int lengthOfRecord = 0;
	int size = attrs.size();

	for(int i = 0; i <size; i++)
	{
		int offset = 0;
		lengthOfRecord =  tableName.length() + attrs[i].name.length() + sizeof(int)*4;
		char* record = new char[lengthOfRecord];
		int tableNameLength = tableName.length();
		memcpy(record, &tableNameLength, sizeof(int));
		offset += sizeof(int);
		memcpy(record+offset , tableName.c_str(), tableName.length());
		offset += tableName.length();
		int attributeNameLength = attrs[i].name.length();
		memcpy(record+ offset, &attributeNameLength, sizeof(int));
		offset += sizeof(int);
		memcpy(record+offset , attrs[i].name.c_str(), attrs[i].name.length());
		offset += attrs[i].name.length();
		memcpy(record+offset, &attrs[i].length, sizeof(int));
		offset += sizeof(int);
		memcpy(record+offset, &attrs[i].type, sizeof(int));

		//insert table info into catalog

		//find which page has free space
		int j = 0;
		int count = 0;
		while(true)
		{
			catalogHandle.ReadPage(directoryNum, page);
			int freeSpaceInfo;
			memcpy(&freeSpaceInfo, page+(j*4), sizeof(int));
			if(lengthOfRecord > freeSpaceInfo)
			{
				j++;
			}
			else if(j == 1023)
			{
				count++;
				int nextDirecotryNumber = count * 1024;
				if((catalogHandle.GetNumberOfPages()-1024*count) == 0)
				{
					prepareNewDirectory(page);
					catalogHandle.AppendPage(page);
				}
				directoryNum = nextDirecotryNumber;
				j = 0;
			}
			else
			{
					//** think again
				if( (catalogHandle.GetNumberOfPages()-1024*count) == (j+1) )
					{
						int a = 0;
						memcpy(page+4096-8, &a, sizeof(int));
						memcpy(page+4096-8+4, &a, sizeof(int));
						catalogHandle.AppendPage(page);
					}
				catalogHandle.ReadPage(j+1, page);
				break;
			}
		}
				//insert table info
				int currentSlotNum;
				memcpy(&currentSlotNum, page + (4096-8)+4, sizeof(int));					//check how many slot are there in slot 0
				int recordSlotNumber = currentSlotNum + 1;
				memcpy(page + (4096-8)+4, &recordSlotNumber, sizeof(int));							//update total slot number
				int recordOffset = 0;
				memcpy(&recordOffset, page + (4096-8*recordSlotNumber), sizeof(int));
				memcpy(page + recordOffset, record, lengthOfRecord);							//put data into file
				int nextRecordOffset;
				nextRecordOffset = recordOffset + lengthOfRecord;
				memcpy(page + 4096 - 8*(recordSlotNumber+1), &nextRecordOffset, sizeof(int));		//put next tuple offset into current slot
				memcpy(page + 4096 - 8*(recordSlotNumber+1) + 4, &lengthOfRecord, sizeof(int));	//put current length of data into slot
				//update free space info
				int newFreeSpace = 4096 - nextRecordOffset - 8*(recordSlotNumber+1);
				char* directory = new char[4096];
				catalogHandle.ReadPage(directoryNum, directory);
				memcpy(directory+4*(j+1-1), &newFreeSpace, sizeof(int));
				catalogHandle.WritePage(directoryNum, directory);
				//catalogHandle.ReadPage(0, directory);
				//memcpy(&newFreeSpace, directory+4*(j+1-1), sizeof(int));

				catalogHandle.WritePage(j+1, page);
				//pf->CloseFile(catalogHandle);

				delete[]directory;
				delete[]record;
	}

	 delete[]page;
	 pf->CloseFile(catalogHandle);
	return 0;
}




RC RM::deleteTableInfo(string tableName)
{
	PF_FileHandle catalogHandle;
	pf->OpenFile("Catalog", catalogHandle);
	 int lengthOfRecord = 0;
	 int offset = 0;
	 int deleteFlag = -1;
	 int numberOfPages = catalogHandle.GetNumberOfPages();
	// int directoryNum = numberOfPages/1024;
	 //char* directory = new char[4096];
	 for(int j = 1; j < numberOfPages; j++){
	//if(j%1024 == 0)
		 //catalogHandle.ReadPage(j, directory);
		 if(j%1024 !=0){
			 char* page= new char[4096];
			 catalogHandle.ReadPage(j,page);
			 int numberOfRecords = 0;
			 memcpy(&numberOfRecords, page+4096-4, sizeof(int));
			 for(int i = 1; i< numberOfRecords+1; i++)
			 {
				 memcpy(&lengthOfRecord, page+4096-8*(i+1)+4, sizeof(int));	//get length of record
				 memcpy(&offset, page+4096-8*i, sizeof(int));	//get offset of record
				 if(lengthOfRecord>0)
				 {
					 char *record = new char[lengthOfRecord];
					 memcpy(record, page+offset, lengthOfRecord);
					 int nameLength =0;
					 memcpy(&nameLength, record, sizeof(int));
					 if(nameLength == tableName.length())
					 {
						 char *name = new char[nameLength+1];
						 name[nameLength] = '\0';
						 memcpy(name, record+sizeof(int), nameLength);
						 if(memcmp(name, tableName.c_str(), nameLength) == 0)
							 memcpy(page+4096-8*(i+1)+4, &deleteFlag, sizeof(int));
						 delete[]name;
					 }
					 delete[]record;
				 }
			 }
			 catalogHandle.WritePage(j, page);
			 delete[]page;
		 }
	 }
	pf->CloseFile(catalogHandle);
	//delete[]directory;

	return 0;
}


/*
 *create new slot for the new tuple
 *make old slot info the same as the new slot info
 *old tuple left in the page with no reference
 */

RC RM::updateTuple(const string tableName, const void *data, const RID &rid)
{
	PF_FileHandle tableHandle;

	pf->OpenFile( tableName.c_str(), tableHandle );
	char * page = new char[4096];
	tableHandle.ReadPage(rid.pageNum, page);
	int lengthOfData;
	int lengthOfCurrentData;
	memcpy(&lengthOfData, page + 4096-8*(rid.slotNum+1) + 4, sizeof(int));

	if(lengthOfData > 4096)
		lengthOfCurrentData = 8;
	else{
		lengthOfCurrentData= getDataLength(tableName, data);
	}

	if (lengthOfCurrentData > lengthOfData)
	{
		//1. update offset and length of data
		//2. update freespace info

		//int currentSlotNum;
		//memcpy(&currentSlotNum, page + (4096-8)+4, sizeof(int));					//check how many slot are there in slot 0
		char* directory = new char[4096];
		int directoryNum = rid.pageNum/512;
		tableHandle.ReadPage(directoryNum*512, directory);
		int currentFreeSpace;
		memcpy( &currentFreeSpace, directory+8*(rid.pageNum%512-1), sizeof(int));
		int currentSlotNum;
		memcpy(&currentSlotNum, directory+8*(rid.pageNum%512-1)+4, sizeof(int));
		if(lengthOfCurrentData + 8  > currentFreeSpace)
			{
			RID id;
			int offset = 0;
			insertTuple(tableName, data, id );
			memcpy(&offset, page + 4096-8*(rid.slotNum), sizeof(int));
			int findAnotherPage = 10000;
			memcpy(page + 4096-8*(rid.slotNum+1) + 4, &findAnotherPage, sizeof(int));
			char *record = new char[8];
			memcpy(record, &id.pageNum, sizeof(int));
			memcpy(record+4, &id.slotNum, sizeof(int));
			memcpy(page + offset, record, 8);
			tableHandle.WritePage(rid.pageNum,page);
			updateTuple(tableName, record, rid);
			delete[]record;
			}else
			{
				int offset;
				memcpy(&offset, page + 4096-8*(currentSlotNum+1), sizeof(int));
				memcpy(page + 4096-8*rid.slotNum, &offset, sizeof(int));	//update new offset in current slot
				memcpy(page+offset, data, lengthOfCurrentData);	//input new data
				memcpy(page + 4096-8*(rid.slotNum+1) + 4, &lengthOfCurrentData, sizeof(int)); //update new length in current slot
				int newOffSet = offset + lengthOfCurrentData;
				memcpy(page + 4096-8*(currentSlotNum+1), &newOffSet, sizeof(int));//update new offset in the last slot
				//update free space info
				int newFreeSpace = 4096 - newOffSet - 8*(currentSlotNum+1);
				memcpy(directory+8*(rid.pageNum%512-1), &newFreeSpace, sizeof(int));
				tableHandle.WritePage(directoryNum*1024, directory);
			}

		delete[]directory;
	}
	else
	{
		int offset;
		memcpy(&offset, page + 4096-8*rid.slotNum, sizeof(int));
		memcpy(page+offset, data, lengthOfCurrentData);	//input new data
		memcpy(page + 4096-8*(rid.slotNum+1) + 4, &lengthOfCurrentData, sizeof(int)); //update new length
	}
	tableHandle.WritePage(rid.pageNum, page);
	pf->CloseFile(tableHandle);
	delete[]page;

	return 0;
}

/*
 *1. open file corresponding to the table
 *2. read the whole page via record's page number
 *3. **read particular tuple from the whole page via record's slot number
 *4. close file
*/
RC RM::readTuple(const string tableName, const RID &rid, void *data)
{
	PF_FileHandle tableHandle;

	if( pf->OpenFile( tableName.c_str(), tableHandle ) != 0)
		return -1;
	char * page = new char[4096];
	tableHandle.ReadPage(rid.pageNum, page);
	int offset;
	memcpy(&offset, page + 4096-8*rid.slotNum, sizeof(int));
	int lengthOfData;
	memcpy(&lengthOfData, page + 4096-8*(rid.slotNum+1) + 4, sizeof(int));
	if (lengthOfData < 0)
	{
		pf->CloseFile(tableHandle);
		return -1;
	}
	else if(lengthOfData >4096)
	{
		RID id;
		char* record = new char[8];
		memcpy(record, page+offset, 8);
		memcpy(&id.pageNum, record, sizeof(int));
		memcpy(&id.slotNum, record+4, sizeof(int));
		readTuple(tableName, id, data);
		delete[]record;
	}else{
		memcpy(data, page+offset, lengthOfData);
	}
	pf->CloseFile(tableHandle);
	delete[]page;

	return 0;
}

/*
 *
 */
RC RM::readAttribute(const string tableName, const RID &rid, const string attributeName, void *data)
{
	//**Think again
	int maxTupleLength = 0;
	vector<Attribute>  attributes;
	getAttributes(tableName, attributes);
	for(unsigned i = 0; i < attributes.size(); i++)
	{
		maxTupleLength += attributes[i].length;
	}
	char* tuple = new char[maxTupleLength];
	readTuple(tableName, rid, tuple);
	int length = getDataLength(tableName, tuple);
	//int test;
	//memcpy(&test, tuple+10, sizeof(int));
	//getDataLength(tableName, tuple);
	int attrPosition = 0;
	//int attributeLength = 0;

	int offset = 0;
	int lengthOfAttribute = 0;
	int i = 0;
	while(true)
	{
		//**Think again. problem may exist in getAttributes
		if(memcmp(attributes[i].name.c_str(), attributeName.c_str(), attributeName.length()) == 0)
			{
			attrPosition = i;
			break;
			}
		else
		{
			if(attributes[i].type == TypeVarChar)
			{
				int a;
				memcpy(&a, tuple + offset, sizeof(int));
				offset += (a + 4);
			}
			else if(attributes[i].type == 0 || attributes[i].type == 1)
			{
				offset += 4;
			}
			i++;
		}
	}
	if(attributes[attrPosition].type == TypeVarChar)
	{
		memcpy(&lengthOfAttribute, tuple + offset, sizeof(int));
		offset += 4;
		memcpy(data, &lengthOfAttribute, sizeof(int));
		memcpy(data+4, tuple + offset, lengthOfAttribute);
	}
	else //if(attributes[attrPosition].type == TypeInt || attributes[attrPosition].type == TypeReal)
	{
		lengthOfAttribute = sizeof(int);
		memcpy(data, tuple + offset, lengthOfAttribute);
	}




	delete []tuple;
	return 0;
}

/*
 *
 */
RC RM::reorganizePage(const string tableName, const unsigned pageNumber)
{
	PF_FileHandle tableHandle;
	if( pageNumber<=0 )
		return -1;
	pf->OpenFile(tableName.c_str(), tableHandle);
	char* directory = new char[4096];
	int directoryNum = pageNumber/512;
	tableHandle.ReadPage(directoryNum, directory);
	int currentSlotNumber = 0;
	memcpy(&currentSlotNumber, directory+(pageNumber%512-1)+4, sizeof(int));
	char* page = new char[4096];
	char* newPage = new char[4096];
	tableHandle.ReadPage(pageNumber, page);
	int lengthOfRecord = 0;
	int offset = 0;
	int newOffset = 0;
	int count = 0;
	for(int i = 0; i < currentSlotNumber; i++)
	{
		memcpy(&lengthOfRecord, page+4096-8*(i+1+1)+4, sizeof(int));
		if(lengthOfRecord >= 0&&lengthOfRecord<4096)
		{
			count++;
			memcpy(&offset, page+4096-8*(i+1), sizeof(int));
			char* record = new char[lengthOfRecord];
			memcpy(record, page+offset, lengthOfRecord);
			//memcpy(newPage+4096-8+4, &count, sizeof(int));	//new page slot number

			memcpy(newPage+4096-8*count, &newOffset, sizeof(int));	//new page record offset
			memcpy(newPage+4096-8*(count+1)+4, &lengthOfRecord, sizeof(int));	//new page record length
			memcpy(newPage+newOffset, record, lengthOfRecord);	//put record in new page

			newOffset += lengthOfRecord;
			delete[]record;
		}
	}
		//update new slot number
	memcpy(directory+8*(pageNumber%512-1), &count, sizeof(int));
		//update free space info
	int nextTupleOffset = newOffset;
	int newFreeSpace = 4096 - nextTupleOffset - 8*(count+1);
	memcpy(directory+8*(pageNumber%512-1), &newFreeSpace, sizeof(int));
	tableHandle.WritePage(directoryNum, directory);
	tableHandle.WritePage(pageNumber, newPage);
	pf->CloseFile(tableHandle);
	delete[]page;
	delete[]newPage;
	delete[]directory;

	return 0;
}

/*
 *1. open file corresponding to table
 *2. check which page is free, assign pageNum
 *3. read whole page
 *4. decide where to put the data(check slot, locate via offset)
 *5. update free space info in directory
 */
RC RM::insertTuple(const string tableName, const void *data, RID &rid)
{
	PF_FileHandle tableHandle;

	pf->OpenFile( tableName.c_str(), tableHandle );

	// tableHandle;= handle;

	if(tableHandle.GetNumberOfPages() == 0)
		{
		char *data1 = new char [PF_PAGE_SIZE];
		prepareNewDirectory(data1);
		tableHandle.AppendPage(data1);
		delete[]data1;
		}
	int directoryNum = 0;
	void *page = malloc(4096);
	int offset;

	tableHandle.ReadPage(0, page);
	pf->CloseFile(tableHandle);
	int lengthOfData = getDataLength(tableName, data);
	pf->OpenFile( tableName.c_str(), tableHandle );
	rid.pageNum = 0;
	int i = 0;
	int count = 0;
	//find which page has free space
	while(true)
	{
		int freeSpaceInfo;
		memcpy(&freeSpaceInfo, (char *)page+(i*8), sizeof(int));

		if(lengthOfData > 4092||lengthOfData<=0)
			{
			pf->CloseFile(tableHandle);
			return -1;
			}
		else if(i == 511)
			{
			prepareNewDirectory(page);
			tableHandle.AppendPage(page);
			count++;
			int nextDirecotryNumber = count * 512;
			//tableHandle.ReadPage(nextDirecotryNumber, page);
			directoryNum = nextDirecotryNumber;
			i = 0;
			}
		else if(lengthOfData + 8 > freeSpaceInfo&&lengthOfData<4092)
			i++;
		else
			{
			//** think again
			if( (tableHandle.GetNumberOfPages()-512*count) == (i+1))
				{
				unsigned a =0;
				//memcpy(page+4096-8+4, &a, sizeof(int));
				memcpy((char *)page+4096-8, &a, sizeof(int));
				//offset = 0;
				//slot.number = 0;
				tableHandle.AppendPage(page);
				}
			rid.pageNum = i+1+count*512;

			break;
			}
	}
	free(page);

	char* directory = new char[4096];
	tableHandle.ReadPage(directoryNum, directory);
	unsigned currentSlotNum = 0;
	memcpy (&currentSlotNum, directory+8*(rid.pageNum-count*512)-4, sizeof(int));					//check how many slot are there in slot 0
	rid.slotNum = ++currentSlotNum ;
	memcpy (directory+8*(rid.pageNum-count*512)-4 , &currentSlotNum,sizeof(int));					//check how many slot are there in slot 0

	char * page2 =new char[4096];
	tableHandle.ReadPage(rid.pageNum, page2);
	char* buffer2 = new char[8];
	memcpy(&offset, (char *)page2+4096-8*rid.slotNum, sizeof(int));
	/*
	int test;
	memcpy(&test, (char*)page2+4096-4, sizeof(int));
	offset = offset<test?offset:test;
	*/
	//memcpy(&buffer2, (char *)page2+4096-8*rid.slotNum,8);
	//memcpy(&offset, buffer2, sizeof(int));
	memcpy ((char *)page2 + offset, data, lengthOfData);							//put data into file
	//delete[]buffer2;
	tableHandle.WritePage(rid.pageNum, page2);
	delete[]page2;

	char * page4 = new char[4096];
	tableHandle.ReadPage(rid.pageNum, page4);
	int nextTupleOffset;
	nextTupleOffset = offset + lengthOfData ;
	char* buffer = new char[8];
	memcpy (buffer, &nextTupleOffset, sizeof(int));		//put next tuple offset into current slot
	memcpy (buffer + 4, &lengthOfData, sizeof(int));	//put current length of data into slot
	memcpy((char*)page4+4096-4, &nextTupleOffset, sizeof(int));
	memcpy((char *)page4 + 4096 - 8*(rid.slotNum+1), buffer, 8);
	delete[]buffer;
	tableHandle.WritePage(rid.pageNum, page4);
	//trace
	tableHandle.ReadPage(rid.pageNum, page4);
	int test2;
	memcpy(&test2, (char *)page4 + 4096 - 8*(rid.slotNum+1), sizeof(int));
	delete[]page4;

	//update free space info
	int newFreeSpace = 4096 - nextTupleOffset - 8*(rid.slotNum+1);

	memcpy (directory+8*(rid.pageNum-512*count-1), &newFreeSpace, sizeof(int));
	tableHandle.WritePage(directoryNum, directory);

	pf->CloseFile(tableHandle);

	delete[]directory;
	//delete[]page2;
	//delete[]page4;


	return 0;
	}

/*
 *
 */
RC RM::deleteTuples(const string tableName)
{
	PF_FileHandle tableHandle;
	pf->OpenFile( tableName.c_str(), tableHandle );

	char * page = new char[4096];
	//int count = tableHandle.GetNumberOfPages()/512;
	//for(int z = 0; z < count+1; z++){
	for(unsigned i = 1; i < tableHandle.GetNumberOfPages(); i++)
	{
		if(i%512 !=0 ){
		char*directory =  new char[4096];
		tableHandle.ReadPage(i/512, directory);
		int currentSlotNumber;
		memcpy(&currentSlotNumber, directory+8*(i%512-1)+4, sizeof(int));
		tableHandle.ReadPage(i,page);
		int deleteFlag = -1;
		for(int j = 1; j <= currentSlotNumber; j++)
		{
			memcpy(page+4096-8*(j+1)+4, &deleteFlag, sizeof(int));
		}
		tableHandle.WritePage(i, page);
		delete[]directory;
		}
	}
	//}
	pf->CloseFile(tableHandle);
	delete []page;

	return 0;
}

/*
 *
 */
RC RM::scan(const string tableName, const string conditionAttribute, const CompOp compOp,
		const void *value, const vector<string> &attributeNames,  RM_ScanIterator &rm_ScanIterator)
{
	PF_FileHandle tableHandle;
	pf->OpenFile( tableName.c_str(), tableHandle );
	unsigned directoryNum = 0;
	rm_ScanIterator.getRM(this);
	rm_ScanIterator.attributeNames = attributeNames;

	for(int i = 0; i < tableHandle.GetNumberOfPages(); i++)
	{
		unsigned pageNum = 0;
		unsigned currentSlotNum = 0;
		int recordOffset = 0;
		int lengthOfRecord =0;
		RID rid;
		char* directory = new char[4096];

		char* page = new char[4096];
		if(i % 512 == 0)
		{
			directoryNum = i;
			i++;
		}
		pageNum = i;
		tableHandle.ReadPage(directoryNum, directory);
		memcpy(&currentSlotNum, directory+ 8*(pageNum-directoryNum)-4, sizeof(int));
		tableHandle.ReadPage(pageNum, page);
		for(int j = 1; j <= currentSlotNum; j++)
		{
			bool isRecord = false;
			int recordLength = 0;
			int recordOffset = 0;
			memcpy(&recordLength, page+4096-(j+1)*8+4, sizeof(int));
			memcpy(&recordOffset, page+4096-j*8, sizeof(int));
			if(recordLength<4096 && recordLength>0)
			{
				char* record = new char[recordLength];
				memcpy(record, page+recordOffset, recordLength);
				int conditionAttributePosition = 0;
				int conditionAttributeLength = 0;
				int offset = 0;
				vector<Attribute> attrs;
				getAttributes(tableName, attrs);
				for(int z = 0; z < attrs.size(); z++)
				{
					if(conditionAttribute.length() == 0)
					{
						break;
					}
					if(memcmp(attrs[z].name.c_str(), conditionAttribute.c_str(), conditionAttribute.length()) != 0)
					{
						if(attrs[z].type == TypeVarChar)
						{
							int length = 0;
							memcpy(&length, record+offset, sizeof(int));
							offset += (4+length);
						}
						else
						{
							offset += sizeof(int);
						}
					}else
					{
						conditionAttributePosition = z;
						if(attrs[z].type == TypeVarChar)
						{
							memcpy(&conditionAttributeLength, record+offset, sizeof(int));
							offset += sizeof(int);
						}else
						{
							conditionAttributeLength = sizeof(int);
						}
						break;
					}
				}

				char * data = new char[conditionAttributeLength];
				memcpy(data, record+offset, conditionAttributeLength);
				if(conditionAttributeLength == 0)
					isRecord = true;
				else if(conditionAttributeLength == sizeof(int))
							{
								switch(compOp)
								{
								//*********** memcmp can not be used to compare two numbers
								case 0:
									{
										if(*(float*)data  == *(float*)value)
											{
											isRecord = true;
											}
										break;
									}
								case 1:
								{
									if(*(float*)data  < *(float*)value)
											isRecord = true;
										break;
								}
								case 2:
								{
									if(*(float*)data  > *(float*)value)
											isRecord = true;
										break;
								}
								case 3:
								{
									if(*(float*)data  <= *(float*)value)
											isRecord = true;
										break;
								}
								case 4:
								{
									if(*(float*)data  >= *(float*)value)
											isRecord = true;
										break;
								}
								case 5:
								{
									if(*(float*)data  != *(float*)value)
											isRecord = true;
										break;
								}
								default:
								{
									isRecord = true;
									break;
								}
							}
						}else
						{
							switch(compOp)
							{
							case 0:
								{
									if(strcmp(data, (char*)value) == 0)
										isRecord = true;
									break;
								}
							case 1:
								{
								if(strcmp(data, (char*)value) < 0)
										isRecord = true;
									break;
								}
							case 2:
								{
								if(strcmp(data, (char*)value) > 0)
										isRecord = true;
									break;
								}
							case 3:
								{
								if(strcmp(data, (char*)value) == 0||strcmp(data, (char*)value) < 0)
										isRecord = true;
									break;
								}
							case 4:
								{
								if(strcmp(data, (char*)value) == 0||strcmp(data, (char*)value) >= 0)
										isRecord = true;
									break;
								}
							case 5:
								{
								if(strcmp(data, (char*)value) != 0)
										isRecord = true;
									break;
								}
							default:
								{
								isRecord = true;
								break;
								}
							}
						}
				if( isRecord )
				{
						rid.pageNum = pageNum;
						rid.slotNum = j;
						rm_ScanIterator.rids.push_back(rid);
						rm_ScanIterator.recordLengths.push_back(recordLength);
						rm_ScanIterator.tableName = tableName;

				}
				delete[]record;
				delete[]data;
			}
		}
		delete[]page;
		delete[]directory;

	}
	pf->CloseFile(tableHandle);
	return 0;
}


/*
 *set length negative if deleted
 */
RC RM::deleteTuple(const string tableName, const RID &rid)
{
	PF_FileHandle tableHandle;
	pf->OpenFile( tableName.c_str(), tableHandle );

	char * page = new char[4096];
	tableHandle.ReadPage(rid.pageNum, page);
	int deleteLength = -1;
	memcpy(page+4096 - 8*(rid.slotNum+1) + 4, &deleteLength, sizeof(int) );
	tableHandle.WritePage(rid.pageNum, page);
	pf->CloseFile(tableHandle);
	delete []page;

	return 0;
}


int RM::getDataLength(const string tableName, const void*data)
	{
		vector<Attribute> attributes;
		getAttributes(tableName, attributes);
		int length = 0;
		int size = attributes.size();
		for(unsigned i = 0; i < size ; i++)
		{
			if(attributes[i].type == TypeVarChar)
				{
					int a;
					memcpy(&a, data, sizeof(int));
					length += (a + 4);
					data += (a + 4);//**
				}//length = length + ((int)attributes[i].length + sizeof(int));
			else if(attributes[i].type == 0 || attributes[i].type == 1)
				{
				length += sizeof(int);
				data += sizeof(int);	//**
				}
		}
		return length;
	}

	/*void RM::prepareNewPage(char* data)
	{
		for(unsigned i = 0; i < 4096; i++)
			*(data+i) = 0;
		for(unsigned i = 0; i < 4096; i++)
			cout<<*(data+i);
	}*/

	void RM::prepareNewDirectory(void * data)
	{
		int a = 4096;
		int b = 0;
		for(unsigned i = 0; i < PF_PAGE_SIZE/8; i++)
		{
			memcpy((char*)data+i*8, &a, sizeof(int));
			memcpy((char*)data+i*8+4, &b, sizeof(int));
		}
	}

	void  RM_ScanIterator::getRM(RM * newRM)
	{
		rm = (RM*) newRM;
	}

	RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
	{
		int offset =0;
		vector<Attribute> attrs;
		vector<Data> dts;
		// trace
		int test = rids.size();
		int recordlength = recordLengths[i];

		rm->getAttributes(tableName, attrs);
		int at = attrs.size();

		if(i >= rids.size())
			return RM_EOF;
		char*record = new char[recordLengths[i]];
		rm->readTuple(tableName,rids[i],record);
		for(int y = 0; y < attributeNames.size(); y++)
			{
				int offset2 = 0;
				Data dt;
				for(int x = 0; x < attrs.size(); x++)
				{
					if(memcmp(attrs[x].name.c_str(), attributeNames[y].c_str(),attributeNames[y].length() ) != 0)
					{
						if(attrs[x].type == TypeVarChar)
						{
							int length = 0;
							memcpy(&length, record+offset2, sizeof(int));
							offset2 += (4+length);
						}
						else
						{
							offset2 += sizeof(int);
						}
					}else
					{
						if(attrs[x].type == TypeVarChar)
						{
							memcpy(&dt.length, record+offset2, sizeof(int));
							offset2 += sizeof(int);
							dt.offset = offset2;
							dt.type = TypeVarChar;
							dts.push_back(dt);
							offset2 += dt.length;
						}else
						{
							dt.length= sizeof(int);
							dt.offset = offset2;
							dt.type = TypeInt;
							dts.push_back(dt);
							offset2 += sizeof(int);
						}
						break;
					}
				}
			}
			for(int j = 0; j < dts.size(); j++)
			{
				if( dts[j].type == TypeInt || dts[j].type == TypeReal)
					{
					memcpy(data+offset, record+dts[j].offset,sizeof(int));
					}

				else
				{
					memcpy(data+offset, record+dts[j].offset-4, sizeof(int));
					offset+=sizeof(int);
					memcpy(data+offset, record+dts[j].offset,dts[j].length);
				}
				offset+=dts[j].length;
			}
			rid = rids[i];
			i++;
		delete[]record;
		return 0;
	}

	 RC RM::dropAttribute(const string tableName, const string attributeName)
	 {
		 PF_FileHandle catalogHandle;
		 pf->OpenFile("Catalog", catalogHandle);
		  int lengthOfRecord = 0;
		  int offset = 0;
		  int deleteFlag = -1;
		  int numberOfPages = catalogHandle.GetNumberOfPages();
		  for(int j = 1; j < numberOfPages; j++){
		 		 if(j%1024 !=0){
		 			 char* page= new char[4096];
		 			 catalogHandle.ReadPage(j,page);
		 			 int numberOfRecords = 0;
		 			 memcpy(&numberOfRecords, page+4096-4, sizeof(int));
		 			 for(int i = 1; i< numberOfRecords+1; i++)
		 			 {
		 				 memcpy(&lengthOfRecord, page+4096-8*(i+1)+4, sizeof(int));	//get length of record
		 				 memcpy(&offset, page+4096-8*i, sizeof(int));	//get offset of record
		 				 if(lengthOfRecord>0)
		 				 {
		 					 char *record = new char[lengthOfRecord];
		 					 memcpy(record, page+offset, lengthOfRecord);
		 					 int nameLength =0;
		 					 memcpy(&nameLength, record, sizeof(int));
		 					 if(nameLength == tableName.length())
		 					 {
		 						 char *name = new char[nameLength+1];
		 						 name[nameLength] = '\0';
		 						 memcpy(name, record+sizeof(int), nameLength);
		 						 if(memcmp(name, tableName.c_str(), nameLength) == 0)
		 							 {
		 							 int attributeNameLength = 0;
		 							 memcpy(&attributeNameLength, record+sizeof(int)+nameLength, sizeof(int));
		 							 if(attributeNameLength == attributeName.length())
		 							 {
		 								 char* attributename = new char[attributeNameLength+1];
		 								 attributename[attributeNameLength] = '\0';
		 								 memcpy(attributename, record+sizeof(int)+nameLength+sizeof(int), attributeNameLength);
		 								 if(memcmp(attributename, attributeName.c_str(), attributeNameLength) == 0)
		 									 memcpy(page+4096-8*(i+1)+4, &deleteFlag, sizeof(int));
		 								 delete[]attributename;
		 							 }
		 							 }
		 						 delete[]name;
		 					 }
		 					 delete[]record;
		 				 }
		 			 }
		 			 catalogHandle.WritePage(j, page);
		 			 delete[]page;
		 		 }
		 	 }
		 	pf->CloseFile(catalogHandle);
		 	//delete[]directory;

		 	return 0;
		 }

	  RC RM::addAttribute(const string tableName, const Attribute attr)
	  {
		  vector<Attribute> attrs;
		  attrs.push_back(attr);
		  insertTableInfo(tableName, attrs);
		  return 0;
	  }

	  RC RM::readAttributes(const string tableName, const RID &rid, vector<string> attributeNames, void *data)
	  {
	  	//**Think again
	  	int maxTupleLength = 0;
	  	int totalLength = 0;
	  	int dataOffset = 0;
	  	vector<Attribute>  attributes;
	  	getAttributes(tableName, attributes);
	  	char* test = new char[1000];
	  	for(unsigned i = 0; i < attributes.size(); i++)
	  	{
	  		maxTupleLength += attributes[i].length;
	  	}
	  	char* tuple = new char[maxTupleLength];
	  	readTuple(tableName, rid, tuple);

	  	int attrPosition = 0;


	  	int lengthOfAttribute = 0;

	  	for(unsigned j = 0; j < attributeNames.size();j++){
	  		int offset = 0;
	  		int i = 0;

	  	while(true)
	  	{
	  		if(memcmp(attributes[i].name.c_str(), attributeNames[j].c_str(), attributeNames[j].length()) == 0)
	  			{
	  			attrPosition = i;
	  			break;
	  			}
	  		else
	  		{
	  			if(attributes[i].type == TypeVarChar)
	  			{
	  				int a;
	  				memcpy(&a, tuple + offset, sizeof(int));
	  				offset += (a + 4);
	  			}
	  			else if(attributes[i].type == TypeInt || attributes[i].type == TypeReal)
	  			{
	  				offset += 4;
	  			}
	  			i++;
	  		}
	  	}
	  	if(attributes[attrPosition].type == TypeVarChar)
	  	{
	  		memcpy(&lengthOfAttribute, tuple + offset, sizeof(int));
	  		offset += 4;
	  		memcpy(test+dataOffset,&lengthOfAttribute, sizeof(int) );
	  		dataOffset += 4;
	  		memcpy(test+dataOffset,tuple+offset, lengthOfAttribute );

	  	}
	  	else //if(attributes[attrPosition].type == TypeInt || attributes[attrPosition].type == TypeReal)
	  	{
	  		lengthOfAttribute = sizeof(int);
	  		memcpy(test+dataOffset, tuple + offset, lengthOfAttribute);

	  	}
	  	dataOffset += lengthOfAttribute;
	  	totalLength += lengthOfAttribute;
	  }

	  	memcpy(data, test, totalLength);

	  	delete []test;
	  	delete []tuple;
	  	return 0;
	  }
	  /*
	  int RM_ScanIterator::getCurrentNumber()
	  {
		  return i;
	  }


	  void RM_ScanIterator::setRID(RID rid)
	  {
		  id.pageNum = rid.pageNum;
		  id.slotNum = rid.slotNum ;
	  }

	  void RM_ScanIterator::setCount(int count)
	  {
		  i = count;
	  }
	  */
	 // RC reorganizeTable(const string tableName);

