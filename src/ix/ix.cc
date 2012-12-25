
#include "ix.h"
#include <stdio.h>
#include <string.h>
#include <cstdlib>

IX_Manager* IX_Manager::_ix_manager = 0;

IX_Manager* IX_Manager::Instance()
{
    if(!_ix_manager)
    	_ix_manager = new IX_Manager();

    return _ix_manager;
}

/*
 *
 */
 RC IX_Manager::CreateIndex(const string tableName,       // create new index
		 const string attributeName)
 {
	 string fileName;
	 fileName = attributeName + "_" +tableName;
	 if(pf->CreateFile(fileName.c_str()) != 0)
		 return -1;
	 prepareIndexFile(fileName);
	 initializeTree(fileName);
	 return 0;
 }

 /*
  *
  */
 RC IX_Manager::DestroyIndex(const string tableName,      // destroy an index
		  const string attributeName)
 {
	 string fileName;
	 fileName = attributeName + "_" + tableName;
	 if( pf->DestroyFile(fileName.c_str()) != 0)
		 return -1;
	 return 0;
 }

 /*
  *
  */
 RC IX_Manager::OpenIndex(const string tableName,         // open an index
 	       const string attributeName, IX_IndexHandle &indexHandle)
 {
	 string fileName;
	 PF_FileHandle fileHandle;
	 fileName = attributeName + "_" + tableName;
	 if(pf->OpenFile(fileName.c_str(), fileHandle)!=0)
		 return -1;
	 indexHandle.setHandle(fileHandle);
	 indexHandle.getTreeInfo();
	 indexHandle.setTableName(tableName);
	 indexHandle.setAttributeName(attributeName);
	 return 0;
 }

 /*
  *
  */
 RC IX_Manager::CloseIndex(IX_IndexHandle &indexHandle)  // close index
 {
	 pf->CloseFile(indexHandle.getHandle());
	 return 0;
 }


 /*
  *store tree info into index file
  */
 void IX_Manager::prepareIndexFile(const string filename)
 {
	 PF_FileHandle handle;
	 pf->OpenFile(filename.c_str(), handle);
	 char* data = new char[4096];
	 int treeHeight = 0;
	 int root = 1;
	 int numberOfFreePages = 0;
	 memcpy(data, &treeHeight, sizeof(int));
	 memcpy(data+4, &root, sizeof(int));
	 memcpy(data+8, &numberOfFreePages, sizeof(int));
	 handle.AppendPage(data);
	 pf->CloseFile(handle);
	 delete[]data;
 }

 /*
  *append the first page in the file; give the root node of the tree
  */
 void IX_Manager::initializeTree(const string filename)
 {
	 PF_FileHandle handle;
	 pf->OpenFile(filename.c_str(), handle);
	 char* data = new char[4096];
	 int nodeHeight = 0;
	 int keyNumber = 0;
	 int offset = 4;
	 memcpy(data+4096-4, &nodeHeight, sizeof(int));
	 memcpy(data+4096-8, &keyNumber, sizeof(int));
	 memcpy(data+4096-12, &offset, sizeof(int));
	 handle.AppendPage(data);
	 pf->CloseFile(handle);
	 delete[]data;
 }

/*
 *
 */
 RC IX_IndexHandle::InsertEntry(void* key, const RID &id)
 {
	//**assume d is known
	 //if(count == 0)
		 //setFanOut(id);
	setAttribute(tableName, attributeName);
	int entrySize = getEntrySize(id);
	char* entry = new char(entrySize);
	prepareEntry(entry,key,id);
	char* nodePage = new char[4096];
	indexHandle.ReadPage(currentPageNumber, nodePage);	//set node page
	int newChildEntrySize = entrySize-4;
	//*****trace
	int testL1,testR1;
	getSiblings(nodePage, testL1, testR1);

	if(!isLeaf(nodePage))	//if this node is noneleaf node
	{
		currentPageNumber = findSubTree(key, nodePage);
		InsertEntry(key, id);

		if (newChildEntry !=NULL)
		{
			int pointer;
			memcpy(&pointer, newChildEntry+newChildEntrySize-4, sizeof(int));
			//*****test
			int test;
			memcpy(&test, nodePage+4096-8, sizeof(int));
			if( newChildEntrySize-4<getFreeSpace(nodePage) )		//test!= 2
			{
				updatePage(nodePage, newChildEntry, newChildEntrySize-4, pointer);
				if(newChildEntry != NULL)
				{
					delete[]newChildEntry;
					newChildEntry = NULL;
				}
				indexHandle.WritePage(currentPageNumber, nodePage);
			}else
			{
				char *L1 = new char[4096];
				char *L2 = new char[4096];
				int leftChild;
				int rightChild;
				getSiblings(nodePage, leftChild, rightChild);
				/*
				int offset;
				memcpy(&offset, nodePage+4096-12, sizeof(int));
				memcpy(&leftChild, nodePage, sizeof(int));
				memcpy(&rightChild,nodePage+offset-4, sizeof(int));*/
				char* splitEntry = new char[entrySize];
				splitPage(nodePage, L1, L2, splitEntry);

				char* entry = new char[newChildEntrySize];
				//memcpy(entry, newChildEntry, newChildEntrySize);
				memcpy(entry, L2+4, newChildEntrySize);

				if(compareKeys(entry, newChildEntry)>=0)
					 updatePage(L2, newChildEntry, newChildEntrySize-4, pointer);
				else
					 updatePage(L1, newChildEntry, newChildEntrySize-4, pointer);

				memcpy(newChildEntry, entry, newChildEntrySize);
				int pageNumberOfN2 = indexHandle.GetNumberOfPages()+1;
				memcpy(newChildEntry+newChildEntrySize-4, &pageNumberOfN2, sizeof(int));
				if(isRoot(currentPageNumber))
				{
					treeHeight++;
					char* data = new char[4096];
					indexHandle.ReadPage(0, data);
					memcpy(data, &treeHeight, sizeof(int));
					char* newRoot = new char[4096];
					createNewRoot(newRoot, newChildEntry, entrySize-8);
					setSiblings(newRoot,currentPageNumber, pageNumberOfN2);
					indexHandle.AppendPage(newRoot);
					int newRootPageNumber = indexHandle.GetNumberOfPages();
					rootPageNumber = newRootPageNumber;
					memcpy(data+4, &newRootPageNumber, sizeof(int));
					indexHandle.WritePage(0, data);
					updateNodeHeight(newRootPageNumber);

					delete[]data;
					delete[]newRoot;
				}
				delete[]entry;
				delete[]splitEntry;
			}
		}
	 }else{												//this node is leaf
		 int test;
		 memcpy(&test, nodePage+4096-8, sizeof(int));
		 if( getEntrySize(id) < getFreeSpace(nodePage) )		//if node has space	test != 4
		 {
			updatePage(nodePage,entry, entrySize, -1);
			if(newChildEntry!=NULL)
			{
				delete[]newChildEntry;
				newChildEntry = NULL;
			}
			indexHandle.WritePage(currentPageNumber, nodePage);
		 }
		 else{
			 char *L1 = new char[4096];
			 char *L2 = new char[4096];
			 int leftChild;
			 int rightChild;
			 int offset;
			 memcpy(&offset, nodePage+4096-12, sizeof(int));
			 getSiblings(nodePage, leftChild, rightChild);
			 char* splitEntry = new char[entrySize];
			 splitPage(nodePage, L1, L2, splitEntry);

			 updatePage(L2, splitEntry, entrySize, -1);

			 newChildEntry = new char[newChildEntrySize];
			 memcpy(newChildEntry, splitEntry, newChildEntrySize);

			 if(compareKeys((char*)key, newChildEntry)>=0)
				 updatePage(L2, entry, entrySize, -1);
			 else
				 updatePage(L1, entry, entrySize, -1);
			 memcpy(newChildEntry, L2+4, newChildEntrySize-4);
			 int pageNumberOfL2 = indexHandle.GetNumberOfPages();
			 memcpy(newChildEntry+newChildEntrySize-4, &pageNumberOfL2, sizeof(int));
			 setSiblings(L1, leftChild, pageNumberOfL2);
			 setSiblings(L2, currentPageNumber, rightChild);

			 if(rightChild>0&&(unsigned)rightChild<indexHandle.GetNumberOfPages())
			 {
				 char* rightNode = new char[4096];
				 indexHandle.ReadPage(rightChild, rightNode);
				 memcpy(rightNode, &pageNumberOfL2, sizeof(int));
				 indexHandle.WritePage(rightChild, rightNode);
				 delete[]rightNode;
			 }

			 //*****trace
			 int testL1,testR1, testL2, testR2;
			 getSiblings(L1, testL1, testR1);
			 getSiblings(L2, testL2, testR2);

			 indexHandle.WritePage(currentPageNumber, L1);
			 indexHandle.AppendPage(L2);
			 if(isRoot(currentPageNumber))	//this leaf node is root
			{
				treeHeight++;
				char* data = new char[4096];
				indexHandle.ReadPage(0, data);
				memcpy(data, &treeHeight, sizeof(int));
				char* newRoot = new char[4096];
				createNewRoot(newRoot, newChildEntry, entrySize-8);
				setSiblings(newRoot,currentPageNumber, pageNumberOfL2);
				indexHandle.AppendPage(newRoot);
				int newRootPageNumber = indexHandle.GetNumberOfPages()-1;
				rootPageNumber = newRootPageNumber;
				memcpy(data+4, &newRootPageNumber, sizeof(int));
				indexHandle.WritePage(0, data);
				updateNodeHeight(newRootPageNumber);
				delete[]data;
				delete[]newRoot;
			}
			delete []L1;
			delete []L2;
			delete []splitEntry;

		 }
	 }
	resetCurrentPagePointer();
	delete[]entry;
	delete[]nodePage;
	return 0;
 }

/*
 *
 */
 RC IX_IndexHandle::DeleteEntry(void *key, const RID &rid)  // Delete index entry
 {
	setAttribute(tableName, attributeName);
	int entrySize = getEntrySize(rid);
	char* entry = new char(entrySize);
	prepareEntry(entry,key,rid);
	char* node = new char[4096];
	indexHandle.ReadPage(currentPageNumber, node);	//set node page

	 if(!isLeaf(node))
	 {
		currentPageNumber = findSubTree(key, node);
		DeleteEntry(key, rid);
	 }
	 else
	 {
		 if(!remove(node, entry))
		 {
			 delete []entry;
			 delete []node;
			 return -1;
		 }
		 if(isEmpty(node))
		 {
			 //2.add page number to free page container
		 }
		 indexHandle.WritePage(currentPageNumber, node);
	 }
	 resetCurrentPagePointer();
	 delete []entry;
	 delete []node;
	 return 0;
 }

 /*
  *	delete algorithm

bool IX_IndexHandle::deletion(char* parent, char* node, char* entry, char* oldChildEntry)
{

	return true;
}*/
/*
 *
 */
bool IX_IndexHandle::remove(char* leaf, char* entry)
{
	char* node = new char[4096];
	//memcpy(node, leaf, 4096);
	int nodeHeight;
	memcpy(&nodeHeight, leaf+4096-4, sizeof(int));
	int keyNumber;
	memcpy(&keyNumber, leaf+4096-8, sizeof(int));
	int offset;
	memcpy(&offset, leaf+4096-12, sizeof(int));
	int entrySize;
	int currentOffset;
	int left, right;
	getSiblings(leaf, left, right);
	for(int i = 1; i <= keyNumber+1; i++)
	{
		if(i == keyNumber+1)
		{
			delete[]node;
			return false;
		}
		memcpy(&currentOffset, leaf+4096-12-i*8, sizeof(int));
		memcpy(&entrySize, leaf+4096-12-i*8+4, sizeof(int));
		char* currentEntry = new char[entrySize];
		memcpy(currentEntry, leaf+currentOffset, entrySize);
		if(compareKeys(entry, currentEntry) == 0)
		{
			memcpy(node, leaf, currentOffset);
			memcpy(node+currentOffset, leaf+currentOffset+entrySize+sizeof(int), offset-currentOffset-entrySize-sizeof(int));
			offset -=(entrySize+sizeof(int));
			if(offset == 4)
				memcpy(node+offset+entrySize, &right, sizeof(int));
			else
				memcpy(node+offset-4, &right, sizeof(int));
			memcpy(node+4096-12, &offset, sizeof(int));
			keyNumber--;
			memcpy(node+4096-8, &keyNumber, sizeof(int));
			memcpy(node+4096-4, &nodeHeight, sizeof(int));
			int slotSize = keyNumber*8;
			char* slot = new char[slotSize];
			memcpy(slot, leaf+4096-12-keyNumber*8, slotSize);
			memcpy(node+4096-12-keyNumber*8, slot, slotSize);
			memcpy(leaf, node, 4096);
			delete[]currentEntry;
			delete[]slot;
			break;
		}
		delete[]currentEntry;
	}
	delete[]node;
	return true;
}

/*
 * tell if a node is empty or not
 */
bool IX_IndexHandle::isEmpty(char* node)
{
	int keyNumber;
	memcpy(&keyNumber, node+4096-8, sizeof(int));
	return keyNumber==0;
}
 /*
  *tree height, root node
  */
 void IX_IndexHandle::getTreeInfo()
 {
	int treeHeight = 0;
	int root = 0;
	int numberOfFreePages;
	char*data = new char[4096];
	indexHandle.ReadPage(0, data);
	memcpy(&treeHeight, data, sizeof(int));
	memcpy(&root, data+4, sizeof(int));
	memcpy(&numberOfFreePages, data+8, sizeof(int));
	for(int i = 0; i < numberOfFreePages; i++)	//get free pages info
	{
		int page;
		memcpy(&page, data+12+i*4, sizeof(int));
		freePages.push_back(page);
	}
	setTreeHeight(treeHeight);
	setRootPage(root);
	delete[]data;
 }

 /*
  * calculate free space with a node page
  */
 int IX_IndexHandle::getFreeSpace(char* page)
 {
	 int offset;
	 int keynumber;
	 int freespace;
	 memcpy(&offset, page+4096-12, sizeof(int));
	 memcpy(&keynumber, page+4096-8, sizeof(int));
	 freespace = 4096 - offset - 12 - keynumber*8 - 4;	//4 is the (d+1)th pointer
	 return freespace;
 }

 /*
  *calculate the size of input entry
  */
 int IX_IndexHandle::getEntrySize(RID id)
 {

	 if(attribute.type == TypeVarChar)
	 {
		 //**THINK AGAIN
		//int maxLength = attribute.length + 4;
		//char* attribute = new char[]
		 return (sizeof(int)+8);
	 }else{
		 return (sizeof(int)+8);
	 }
 }
/*
 * set the attribute given the name of the attribute
 */
 void IX_IndexHandle::setAttribute(string tableName, string attributeName)
 {
	 vector<Attribute> attrs;
	 rm->getAttributes(tableName, attrs);
	 for(unsigned i = 0; i < attrs.size(); i++)
	 {
		 if(strcmp(attributeName.c_str(), attrs[i].name.c_str()) == 0)
		 {
			 attribute = attrs[i];
		 }
	 }
 }

 /*
  * update a slot information(offset, length of data) in a page
  */
 void IX_IndexHandle::updatePage(char* page, char* entry, int entrySize, int pointer)
 {
	 int offset;
	 int keyNumber;
	 int currentOffset;
	 memcpy(&offset, page+4096-12, sizeof(int));
	 memcpy(&keyNumber, page+4096-8, sizeof(int));
	 if(attribute.length == sizeof(int)){
		 //int entrySize = getEntrySize(id);
		 //char* entry = new char(entrySize);
		 //prepareEntry(entry,key,id);
		//int inputKey = getKey(entry);
		 char* previousEntry = new char[entrySize];
		 for(int i = 0; i < keyNumber+1; i++)
		 {
			 if(i == keyNumber)		//add the entry at the end
			 {
				 memcpy(page+offset, entry, entrySize);//put entry in page
				 keyNumber++;
				 memcpy(page+4096-8, &keyNumber, sizeof(int));	//update slot number
				 memcpy(page+4096-12-keyNumber*8, &offset, sizeof(int));	//update slot entry offset
				 memcpy(page+4096-12-keyNumber*8+4, &entrySize, sizeof(int));//update slot entry length
				 offset+=(sizeof(int)+entrySize);
				 if(pointer != -1)
					 memcpy(page+offset-4, &pointer, sizeof(int));	//set the pointer of the entry
				 memcpy(page+4096-12, &offset, sizeof(int));	//update offset
				 break;
			 }
			 memcpy(&currentOffset, page+4096-12-(i+1)*8, sizeof(int));
			 memcpy(previousEntry, page+currentOffset, entrySize);
			 //trace
			 int test;
			 memcpy(&test, previousEntry, sizeof(int));
			 int test2;
			 memcpy(&test2, entry, sizeof(int));

			 if(compareKeys(entry, previousEntry)<=0)
				 {
				 	 int blockSize = offset-currentOffset;
				 	 char* block = new char[blockSize];
				 	 memcpy(block, page+currentOffset, blockSize);
				 	 memcpy(page+currentOffset, entry, entrySize);//put entry in page
				 	 currentOffset += (sizeof(int)+entrySize);
				 	 memcpy(page+currentOffset-4, &pointer, sizeof(int));	//set next pointer
				 	 memcpy(page+currentOffset, block, blockSize);
				 	 keyNumber++;
				 	memcpy(page+4096-8, &keyNumber, sizeof(int));	//update slot number
				 	memcpy(page+4096-12-keyNumber*8, &offset, sizeof(int));	//update slot entry offset
				 	memcpy(page+4096-12-keyNumber*8+4, &entrySize, sizeof(int));//update slot entry length
				 	offset +=(4+entrySize);	//plus next pointer
				 	//memcpy(page+offset-4, &pointer, sizeof(int));	//set next pointer
				 	memcpy(page+4096-12, &offset, sizeof(int));	//update offset

				 	 delete[]block;
				 break;
				 }
		 }
		 delete[]previousEntry;
	 }else
	 {
		 //**
	 }

 }

 /*
  * get the key given the rid of a tuple, turn it into an entry
  */
void IX_IndexHandle::prepareEntry(char* entry,void*key, RID id)
{
	if(attribute.type == TypeVarChar)
	{
		//**
	}else{
		memcpy(entry, key, sizeof(int));
		memcpy(entry+sizeof(int), &id.pageNum, sizeof(int));
		memcpy(entry+2*sizeof(int),&id.slotNum, sizeof(int));
	}
}

/*
 * get key from an entry
 */
int IX_IndexHandle::compareKeys(char* key, char* entry)
{
	if(attribute.type == TypeVarChar){
		//**
		return memcmp(key, entry, sizeof(int));
	}
	else// if(	attribute.type == TypeInt)
	{
		//***********
		//memcmp can not be used to compare two numbers
		float a, b;
		memcpy(&a, key, sizeof(int));
		memcpy(&b, entry, sizeof(int));
		if(a>b)
			return 1;
		else if(a<b)
			return -1;
		else
			return 0;
	}
}


/*
 * split one page into two pages
 */
void IX_IndexHandle::splitPage(char* nodePage, char* L1, char* L2, char* splitEntry)
{
	int keyNumber;
	int offset;
	int nodeHeight;
	memcpy(&nodeHeight, nodePage+4096-4, sizeof(int));
	setNodeHeight(L1, nodeHeight);
	setNodeHeight(L2, nodeHeight);
	memcpy(&keyNumber, nodePage+4096-8, sizeof(int));
	memcpy(&offset, nodePage+4096-12, sizeof(int));
	int currentSlot = keyNumber/2+1;
	int currentOffset;
	memcpy(&currentOffset, nodePage+4096-12-currentSlot*8, sizeof(int));
	int blockSize1 = currentOffset;
	int newKeyNumber1 = currentSlot-1;
	int slotSize1 = newKeyNumber1*8;
	int newOffset1 = currentOffset;

	char* block1 = new char[blockSize1];
	char* slot1 = new char[slotSize1];
	memcpy(block1, nodePage, blockSize1);
	memcpy(slot1, nodePage+4096-12-8*newKeyNumber1, slotSize1);
	memcpy(L1, block1, blockSize1);
	memcpy(L1+4096-12-newKeyNumber1*8, slot1, slotSize1);
	memcpy(L1+4096-8, &newKeyNumber1, sizeof(int));
	memcpy(L1+4096-12, &newOffset1, sizeof(int));

	int splitOffset;
	memcpy(&splitOffset, nodePage+4096-12-(currentSlot+1)*8, sizeof(int));
	splitOffset -=sizeof(int);
	memcpy(splitEntry, nodePage+currentOffset, splitOffset-currentOffset);

	int blockSize2 = offset-splitOffset;
	int newKeyNumber2 = keyNumber-currentSlot;
	int slotSize2 = newKeyNumber2*8;
	int newOffset2 = blockSize2;
	char* block2 = new char[blockSize2];
	char* slot2 = new char[slotSize2];
	memcpy(block2,nodePage+splitOffset,blockSize2);
	memcpy(slot2, nodePage+4096-12-8*newKeyNumber2, slotSize2);
	memcpy(L2, block2, blockSize2);
	memcpy(L2+4096-12-newKeyNumber2*8, slot2, slotSize2);
	memcpy(L2+4096-8, &newKeyNumber2, sizeof(int));
	memcpy(L2+4096-12, &newOffset2, sizeof(int));

	delete []block1;
	delete []block2;
	delete []slot1;
	delete []slot2;
}

/*
 *set the very left and right pointers
 */
void IX_IndexHandle::setSiblings(char* page, int leftChild, int rightChild)
{
	int offset;
	memcpy(&offset, page+4096-12, sizeof(int));
	memcpy(page, &leftChild, sizeof(int));
	memcpy(page+offset-4, &rightChild, sizeof(int));
}

/*
 *create a new root with a given entry
 */
void IX_IndexHandle::createNewRoot(char* page, char* entry, int entrySize)
{
	int offset =4+entrySize+4;
	int currentOffset = 4;
	int keyNumber =1;
	int nodeHeight =0;
	memcpy(page+currentOffset, entry, entrySize);
	memcpy(page+4096-12, &offset, sizeof(int));
	memcpy(page+4096-8, &keyNumber, sizeof(int));
	memcpy(page+4096-4, &nodeHeight, sizeof(int));
	memcpy(page+4096-12-8, &currentOffset, sizeof(int));
	memcpy(page+4096-12-4, &entrySize, sizeof(int));
}

/*
 *
 */
void IX_IndexHandle::updateNodeHeight(int root)
{
	for(unsigned i = 1; i < indexHandle.GetNumberOfPages(); i++)
	{
		char* node = new char[4096];
		if(i != (unsigned)root)
		{
			indexHandle.ReadPage(i, node);
			int currentHeight;
			memcpy(&currentHeight, node+4096-4, sizeof(int));
			currentHeight++;
			memcpy(node+4096-4, &currentHeight, sizeof(int));
			indexHandle.WritePage(i, node);
		}
		delete[]node;
	}
}

/*
 *
 */
void IX_IndexHandle::setNodeHeight(char* node, int height)
{
	memcpy(node+4096-4, &height, sizeof(int));
}

/*
 *
 */
bool IX_IndexHandle::isRoot(int pageNumber)
{
	return pageNumber == rootPageNumber;
}

/*
 *
 */
bool IX_IndexHandle::isLeaf(char* page)
{
	int nodeHeight;
	memcpy(&nodeHeight, page+4096-4, sizeof(int));
	return nodeHeight == getTreeHeight();
}

/*
 * find algorithm
 */
int IX_IndexHandle::findSubTree(void*key, char* page)
{
	int result;
	int keyNumber;
	memcpy(&keyNumber, page+4096-8, sizeof(int));

	int offset;
	memcpy(&offset, page+4096-12-8, sizeof(int));
	int entrySize;
	memcpy(&entrySize, page+4096-12-8+4, sizeof(int));
	char* entry = new char[entrySize];
	memcpy(entry, page+offset, entrySize);
	if(compareKeys((char*)key, entry) < 0)
	{
		memcpy(&result, page, sizeof(int));
	}else
	{
		memcpy(&offset, page+4096-12-keyNumber*8, sizeof(int));
		memcpy(&entrySize, page+4096-12-keyNumber*8+4, sizeof(int));
		memcpy(entry, page+offset, entrySize);
		if(compareKeys((char*)key, entry) >= 0)
		{
			int last;
			memcpy(&last, page+4096-12, sizeof(int));
			memcpy(&result, page+last-4, sizeof(int));
		}else{
			for(int i = 0; i < keyNumber-1; i++)
			{
				memcpy(&offset, page+4096-12-(i+1)*8, sizeof(int));
				memcpy(&entrySize, page+4096-12-(i+1)*8+4, sizeof(int));
				memcpy(entry, page+offset, entrySize);
				int offset2;
				memcpy(&offset2, page+4096-12-(i+2)*8, sizeof(int));
				int entrySize2;
				memcpy(&entrySize2, page+4096-12-(i+2)*8+4, sizeof(int));
				char* entry2 = new char[entrySize2];
				memcpy(entry2, page+offset2, entrySize2);
				if(compareKeys((char*)key, entry) >=0 && compareKeys((char*)key, entry2) < 0)
				{
					memcpy(&result,page+offset2-4, sizeof(int));
				}
			}
		}
	}
	delete[]entry;
	return result;
}

/*
 * reset the pointer to current page to point root page
 */
void IX_IndexHandle::resetCurrentPagePointer()
{
	currentPageNumber = rootPageNumber;
}

/*
 * get the very left and very right pointers of a page
 */
void IX_IndexHandle::getSiblings(char* page, int& left, int& right)
{
	int offset;
	memcpy(&offset, page+4096-12, sizeof(int));
	memcpy(&left, page, sizeof(int));
	memcpy(&right, page+offset-4, sizeof(int));
}

/*
 * find the leaf node with given key value
 */
int IX_IndexHandle::find(char* root, void* key)
{
	int current = rootPageNumber;
	char* node = new char[4096];
	memcpy(node, root, 4096);
	while(!isLeaf(node))
	{
		current =findSubTree(key, node);
		/*
		for(int i = 0; i < freePages.size(); i++)
		{
			if(current == freePages[i])
				current = -1;
		}*/
		indexHandle.ReadPage(current, node);
	}
	delete[]node;
	return current;
}
/*
 *
 */
bool IX_IndexHandle::isOpen()
{
	return indexHandle.pFile != NULL;
}

/*
 * scan according to condition, store all records
 */
void IX_IndexHandle::scan(CompOp operation, void* value, list<RID>& ids)
{
	char* leafPage = new char[4096];
	if(value == NULL)
	{
		//*******trace
		//unsigned test = indexHandle.GetNumberOfPages();
		for(unsigned i = 1; i < indexHandle.GetNumberOfPages(); i++)
		{
			indexHandle.ReadPage(i, leafPage);
			if(isLeaf(leafPage))
			{
				scanOnePage(leafPage, operation, value, ids);
			}
		}
	}else
	{
		char* root = new char[4096];
		indexHandle.ReadPage(rootPageNumber, root);
		int leafPageNumber = find(root, value);
		/*
		if(leafPageNumber == -1)		// the node has been deleted
		{
			delete[]root;
			return false;
		}*/
		indexHandle.ReadPage(leafPageNumber, leafPage);
		//int right, left;
		//getSiblings(leafPage, left, right);
		switch(operation)
		{
		case 0:
		{
			//scanOnePage(leafPage, operation, value, ids);
			for(unsigned i = 1; i < indexHandle.GetNumberOfPages(); i++)
			{
				indexHandle.ReadPage(i, leafPage);
				if(isLeaf(leafPage))
				{
					scanOnePage(leafPage, operation, value, ids);
				}
			}
			break;
		}
		//seek left
		case 1:			// <
		case 3:			// <=
		{
		/*	while(true)		//if there is a left sibling	<
			{
				scanOnePage(leafPage, operation, value, ids);
				getSiblings(leafPage, left, right);
				if(!(left<(int)indexHandle.GetNumberOfPages() && left>0))
					break;
				indexHandle.ReadPage(left, leafPage);
			}*/
			for(unsigned i = 1; i < indexHandle.GetNumberOfPages(); i++)
			{
				indexHandle.ReadPage(i, leafPage);
				if(isLeaf(leafPage))
				{
					scanOnePage(leafPage, operation, value, ids);
				}
			}
			break;
		}
		//seek right
		case 2:			// >
		case 4:			// >=
		{
			/*
			while(true)		//if there is a right sibling	>
			{
				scanOnePage(leafPage, operation, value, ids);
				getSiblings(leafPage, left, right);
				if(!(right<(int)indexHandle.GetNumberOfPages() && right>0))
					break;
				indexHandle.ReadPage(right, leafPage);
			}*/
			for(unsigned i = 1; i < indexHandle.GetNumberOfPages(); i++)
			{
				indexHandle.ReadPage(i, leafPage);
				if(isLeaf(leafPage))
				{
					scanOnePage(leafPage, operation, value, ids);
				}
			}
			break;
		}
		//seek all leaves
		case 5:
		default:
		{
			for(unsigned i = 1; i < indexHandle.GetNumberOfPages(); i++)
			{
				indexHandle.ReadPage(i, leafPage);
				if(isLeaf(leafPage))
				{
					scanOnePage(leafPage, operation, value, ids);
				}
			}
			break;
		}

		}
		delete[]root;
	}
	delete[]leafPage;
}
/*
 * scan one page
 */
void IX_IndexHandle::scanOnePage(char* leafPage, CompOp operation, void* value, list<RID>& ids)
{

	int keyNumber;
	RID id;
	int currentOffset, lengthOfEntry;

	memcpy(&keyNumber, leafPage+4096-8, sizeof(int));

	for(int i = 1; i <=keyNumber ; i++)
	{
		memcpy(&currentOffset, leafPage+4096-12-8*i, sizeof(int));
		memcpy(&lengthOfEntry, leafPage+4096-12-8*i+4, sizeof(int));
		char*entry = new char[lengthOfEntry];
		memcpy(entry, leafPage+currentOffset, lengthOfEntry);
		switch(operation)
		{
		case 0:
		{
			if(compareKeys(entry,(char*) value)==0)
			{
				getRID(entry, id);
				ids.push_back(id);
			}
			break;
		}
		case 1:
		{
			if(compareKeys(entry,(char*) value)<0)
			{
				getRID(entry, id);
				ids.push_back(id);
			}
			break;
		}
		case 2:
		{
			if(compareKeys(entry,(char*) value)>0)
			{
				getRID(entry, id);
				ids.push_back(id);
			}
			break;
		}
		case 3:
		{
			if(compareKeys(entry,(char*) value)<=0)
			{
				getRID(entry, id);
				ids.push_back(id);
			}
			break;
		}
		case 4:
		{
			if(compareKeys(entry,(char*) value)>=0)
			{
				getRID(entry, id);
				ids.push_back(id);
			}
			break;
		}
		case 5:
		{
			if(compareKeys(entry,(char*) value)!=0)
			{
				getRID(entry, id);
				ids.push_back(id);
			}
			break;
		}
		default:
		{
			getRID(entry, id);
			ids.push_back(id);
			break;
		}
		}
		delete[]entry;
	}
}

/*
 * get id from an entry
 */
void IX_IndexHandle::getRID(char* entry, RID& id)
{
	if(attribute.type == TypeVarChar)
	{
	}else
	{
		memcpy(&id.pageNum, entry+4, sizeof(int));
		memcpy(&id.slotNum, entry+8, sizeof(int));
	}
}

/*
 *
void IX_IndexHandle::getScanFileName(string &name) const
{
	name = ("scan"+"_"+attributeName + "_" +tableName);
}
*/

/*
 * index scan
 */
RC IX_IndexScan::OpenScan(const IX_IndexHandle &indexHandle, // Initialize index scan
	      CompOp      compOp,
	      void        *value)
{
	*handle = indexHandle;
	if(!handle->isOpen())
			return -1;
	handle->scan(compOp, value, ids);
	return 0;
}

/*
 *
 */
RC IX_IndexScan::GetNextEntry(RID &rid)  // Get next matching entry
{
	if(ids.empty())
	{
		return IX_EOF;
	}else
	{
		rid = ids.front();
		ids.pop_front();
		return 0;
	}
}

/*
 *
 */
RC IX_IndexScan::CloseScan()             // Terminate index scan
{
	ids.clear();
	return 0;
}


void IX_PrintError (RC rc)
{
	if(rc ==-1)
		cout<<"Error";
}
