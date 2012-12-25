#include "pf.h"

PF_Manager* PF_Manager::_pf_manager = 0;


PF_Manager* PF_Manager::Instance()
{
    if(!_pf_manager)
        _pf_manager = new PF_Manager();
    
    return _pf_manager;    
}


PF_Manager::PF_Manager()
{
}


PF_Manager::~PF_Manager()
{
}

 /*
  * Precondition: File does not exist.
  * Postcondition:Create file with name fileName
  */
RC PF_Manager::CreateFile(const char *fileName)
{

		//char pageBuffer[PF_PAGE_SIZE];
		FILE *pFile;
		pFile = fopen(fileName,"r"); //Create an empty file for both reading and writing.
		if (pFile!=NULL)
		  {
		    perror("File exists.");
			fclose (pFile);
			return -1;
		  }else{
			  pFile = fopen(fileName, "wb+");
			  fclose(pFile);
			  return 0;
		  }
		if(pFile != NULL)
			return 0;
		else
			return -1;
}

/*
 *Precondition: File exists.
 *Postcondition: Delete the existing file. If file does not exist, report error.
 */
RC PF_Manager::DestroyFile(const char *fileName)
{
	int rc  = remove( fileName ) ;
	if(rc != 0 )
	{
		perror("Error deleting file");
		return -1;
	}
	else
	    {
		//cout<< "File successfully deleted"<<endl;
		 return 0;
	    }
}

/*
 * Precondition: File exists
 * Postcondition: File opened.
 */
RC PF_Manager::OpenFile(const char *fileName, PF_FileHandle &fileHandle)
{
	//if(fileHandle.pFile!=NULL)
		//fclose(fileHandle.pFile);
	fileHandle.pFile = fopen(fileName,"r+");              //Open an existing file for both reading and writing.
	if (fileHandle.pFile != NULL)
	{
		//cout<<"Open file successful."<<endl;
		fseek(fileHandle.pFile, 0, SEEK_END);
		fileHandle.totalPage = ftell(fileHandle.pFile)/4096;
		//fseek(fileHandle.pFile, 0, SEEK_SET);
		rewind(fileHandle.pFile);
		return 0;
	}
	else
	{
		perror("Fail to open file.");
		return -1;
	}
}


RC PF_Manager::CloseFile(PF_FileHandle &fileHandle)
{
	if(fileHandle.pFile != NULL)
		{
		fclose(fileHandle.pFile);
		//cout<<"Close"<<endl;
		}
	return 0;
}


PF_FileHandle::PF_FileHandle()
{
	pFile = NULL;
	totalPage = 0;
}
 

PF_FileHandle::~PF_FileHandle()
{
}


RC PF_FileHandle::ReadPage(PageNum pageNum, void *data)
{
	if(pFile == NULL)
	{
		perror("Fail to read file");
		return -1;
	}
	rewind (pFile);
	fseek(pFile, pageNum * PF_PAGE_SIZE, SEEK_SET);
	fread (data,1,PF_PAGE_SIZE,pFile);
	return 0;

}

RC PF_FileHandle::WritePage(PageNum pageNum, const void *data)
{
	if(pFile == NULL)
	{
		perror("Fail to write");
		return -1;
	}
	rewind (pFile);
	fseek(pFile, pageNum * PF_PAGE_SIZE, SEEK_SET);
	fwrite (data,1,PF_PAGE_SIZE, pFile );
    return 0;
}


RC PF_FileHandle::AppendPage(const void *data)
{
	fseek(pFile, GetNumberOfPages() * PF_PAGE_SIZE, SEEK_SET);
	if(data == NULL)
	{
		cout<<"No data!";
		return -1;
	}
	fwrite (data, 1, PF_PAGE_SIZE, pFile);
	totalPage++;
    return 0;
}


unsigned PF_FileHandle::GetNumberOfPages()
{
    return totalPage ;
}




