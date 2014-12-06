#include "catalog.h"

RelCatalog::RelCatalog(Status &status) :
	 HeapFile(RELCATNAME, status)
{
  cerr <<  "[RelCatalog]" << endl;
// nothing should be needed here
}


const Status RelCatalog::getInfo(const string & relation, RelDesc &record)
{
  cerr <<  "[RelCatalog::getInfo] begin for " << relation << endl;
  if (relation.empty())
    return BADCATPARM;

  Status status;
  Record rec;
  RID rid;

  string filename(RELCATNAME);
  HeapFileScan* hfs = new HeapFileScan(filename, status);
  if(status != OK)  return status;
  //Start Scan
  status = hfs->startScan(0, 0, STRING, NULL, EQ);
  if(status != OK)
    return status;
  //Look up the record in the file
  RelDesc* reldesc;
  while((status = hfs->scanNext(rid)) == OK){
    status = hfs->getRecord(rec);
    cerr <<  rec.length << endl;
    reldesc = (RelDesc*)rec.data;
    cerr <<  "[RelCatalog::getInfo] check " << rec.length << (char*)rec.data <<  relation.c_str() << " " << reldesc->relName << endl;
    if(strcmp(relation.c_str(), reldesc->relName) == 0){
      cerr <<  "[RelCatalog::getInfo] found :" << relation << endl;
      memcpy((void*)&record, (void*)reldesc, sizeof(RelDesc));
      break;
    }
  }
  //If not found, return relnotfound
  //return RELNOTFOUND;
  delete hfs;
  cerr <<  "[RelCatalog::getInfo] finished "<<endl;
  if(status != OK)
    return RELNOTFOUND;
  else
    return status;
}

/*
 Adds the relation descriptor contained in record to the relcat relation. RelDesc represents both the in-memory format and on-disk format of a tuple in relcat.
 */

const Status RelCatalog::addInfo(RelDesc & record)
{
  cerr <<  "[RelCatalog::addInfo] begin " << record.relName <<  endl;
  RID rid;
  InsertFileScan*  ifs;
  Status status;
  
  //create an InsertFileScan object on the relation catalog table
  string relation(RELCATNAME);
  ifs = new InsertFileScan(relation, status);
  if(status != OK)  return status;

  //create a new record
  RelDesc* newRel = new RelDesc();
  newRel->attrCnt = record.attrCnt;
  memcpy(newRel->relName, record.relName, MAXNAME);
  
  Record newRecord;
  newRecord.data = (void*)newRel;
  newRecord.length = sizeof(RelDesc);
  //insert the new record to file
  cerr <<  "[RelCatalog::addInfo] insert " ;
  cerr <<  newRecord.length << (char*)newRecord.data << endl;
  status = ifs->insertRecord(newRecord, rid);
  delete ifs;
  cerr <<  "[RelCatalog::addInfo] finished" << endl;
  return status;
}

const Status RelCatalog::removeInfo(const string & relation)
{
  cerr <<  "[RelCatalog::removeInfo] begin " << relation << endl;
  Status status;
  RID rid;
  HeapFileScan*  hfs;
  Record rec;
  
  if (relation.empty()) return BADCATPARM;
  
  string filename(RELCATNAME);
  hfs = new HeapFileScan(filename, status);
  //Start Scan
  status = hfs->startScan(0, 0, STRING, NULL, EQ);
  if(status != OK)
    return status;
  //Look up the record in the file
  RelDesc* reldesc;
  while((status = hfs->scanNext(rid)) == OK){
    status = hfs->getRecord(rec);
    reldesc = (RelDesc*)rec.data;
    cerr <<  "[RelCatalog::removeInfo] check " << relation << " " << reldesc->relName << endl;
    //If found, delete it
    if(strcmp(relation.c_str(), reldesc->relName) == 0){
      status = hfs->deleteRecord();
      cerr <<  "[RelCatalog::removeInfo] remove " << relation << endl;
      break;
    }
  }
  delete hfs;
  //If not found, return relnotfound
  cerr <<  "[RelCatalog::removeInfo] finished" << endl;
  return status == OK ? OK:RELNOTFOUND;

}


RelCatalog::~RelCatalog()
{
  cerr << "[~RelCatalog]" << endl;
// nothing should be needed here
}


AttrCatalog::AttrCatalog(Status &status) :
	 HeapFile(ATTRCATNAME, status)
{
  cerr << "[AttrCatalog]" <<endl;
// nothing should be needed here
}


const Status AttrCatalog::getInfo(const string & relation, 
				  const string & attrName,
				  AttrDesc &record)
{
  cerr <<  "[AttrCatalog::getInfo] begin " << relation << " " << attrName <<  endl;

  Status status;
  RID rid;
  Record rec;
  HeapFileScan*  hfs;

  if (relation.empty() || attrName.empty()) return BADCATPARM;
  
  string filename(ATTRCATNAME);
  hfs = new HeapFileScan(filename, status);
  //Start Scan
  status = hfs->startScan(0, 0, STRING, NULL, EQ);
  if(status != OK)
    return status;
  //Look up the attribute in the file
  AttrDesc* attrdesc;
  while((status = hfs->scanNext(rid)) == OK){
    status = hfs->getRecord(rec);
    attrdesc = (AttrDesc*)rec.data;
    cerr <<  "[AttrCatalog::getInfo] Check "<< relation.c_str() << " " << attrdesc->relName << " ";
    cerr <<  attrName.c_str() << " " << attrdesc->attrName << endl;
    if(strcmp(relation.c_str(), attrdesc->relName) == 0
       && strcmp(attrName.c_str(),attrdesc->attrName) == 0){
      memcpy((void*)&attrdesc, (void*)&rec, sizeof(AttrDesc));
      break;
    }
  }
  delete hfs;
  cerr <<  "[AttrCatalog::getInfo] finished" << endl;
  //If not found, return relnotfound
  return status == OK ? OK : ATTRNOTFOUND;

}


const Status AttrCatalog::addInfo(AttrDesc & record)
{
  cerr <<  "[AttrCatalog::addInfo] begin " << record.relName << " : " << record.attrName<< endl;
  RID rid;
  InsertFileScan*  ifs;
  Status status;
  
  //create an InsertFileScan object on the attribute catalog table
  string attrlog(ATTRCATNAME);
  ifs = new InsertFileScan(attrlog, status);
  if(status != OK)  return status;
  
  //create a new record
  AttrDesc* newAttr = new AttrDesc();
  memcpy((void*)newAttr, (void*)&record, sizeof(AttrDesc));
  
  Record newRecord;
  newRecord.data = (void*)newAttr;
  newRecord.length = sizeof(AttrDesc);
  //insert the new record to file
  status = ifs->insertRecord(newRecord, rid);
  delete ifs;
  cerr <<  "[AttrCatalog::addInfo] finished " << endl;
  return status;
}

//Removes the tuple from attrcat that corresponds to attribute attrName of relation. Implement this function in catalog.cpp.

const Status AttrCatalog::removeInfo(const string & relation, 
			       const string & attrName)
{
  cerr <<  "[AttrCatalog::removeInfo] begin " << relation << " " << attrName<< endl;
  Status status;
  Record rec;
  RID rid;
  AttrDesc record;
  HeapFileScan*  hfs;

  if (relation.empty() || attrName.empty()) return BADCATPARM;
  string filename(ATTRCATNAME);
  hfs = new HeapFileScan(filename, status);
  //Start Scan
  status = hfs->startScan(0, 0, STRING, NULL, EQ);
  if(status != OK)
    return status;
  //Look up the record in the file
  AttrDesc* attrdesc;
  while((status = hfs->scanNext(rid)) == OK){
    status = hfs->getRecord(rec);
    attrdesc = (AttrDesc*)rec.data;
    cerr <<  "[AttrCatalog::removeInfo] " << relation << " " << attrdesc->relName << endl;
    //If found, delete it
    if(strcmp(relation.c_str(), attrdesc->relName) == 0 && strcmp(attrName.c_str(), attrdesc->attrName) == 0){
      status = hfs->deleteRecord();
      cerr << "[AttrCatalog::removeInfo] remove " << relation << " " <<  attrdesc->attrName << endl;
      break;
    }
    
  }
  delete hfs;
  cerr <<  "[AttrCatalog::removeInfo] finished" << endl;
  //If not found, return attrnotfound
  return status == OK ? OK : ATTRNOTFOUND;
}


const Status AttrCatalog::getRelInfo(const string & relation, 
				     int &attrCnt,
				     AttrDesc *&attrs)
{
  cerr <<  "[AttrCatalog::getRelInfo] begin " << relation << endl;
  Status status;
  RID rid;
  Record rec;
  HeapFileScan*  hfs;

  if (relation.empty()) return BADCATPARM;

  //Allocate the arary of attrs
  RelDesc reldesc;
  status = relCat->getInfo(relation, reldesc);
  if(status != OK)  return status;
  attrs = new AttrDesc[reldesc.attrCnt];
  attrCnt = reldesc.attrCnt;
  cerr <<  "[AttrCatalog::getRelInfo] Attr Count " << reldesc.attrCnt << endl;
  string filename(ATTRCATNAME);
  hfs = new HeapFileScan(filename, status);
  //Start Scan
  status = hfs->startScan(0, 0, STRING, NULL, EQ);
  if(status != OK)
    return status;
  //Look up all attribute in the file
  int count = 0;
  bool found = false;
  while((status = hfs->scanNext(rid)) == OK){
    status = hfs->getRecord(rec);
    AttrDesc* attrdesc = (AttrDesc*)rec.data;
    cerr << "[AttrCatalog::getRelInfo] check " << relation << " " << attrdesc->relName << endl;
    if(strcmp(relation.c_str(), attrdesc->relName) == 0){
      memcpy((void*)&attrs[count++],attrdesc,sizeof(AttrDesc));
      cerr <<  "[AttrCatalog::getRelInfo] Attribute Info " << attrs[count-1].relName <<" " << attrs[count-1].attrName << endl;
      found = true;
    }
  }
  delete hfs;
  //If not found, return relnotfound
  cerr <<  "[AttrCatalog::getRelInfo] finished" << endl;
  return found? OK :ATTRNOTFOUND;


}


AttrCatalog::~AttrCatalog()
{
  cerr << "[~AttrCatalog]" << endl;
// nothing should be needed here
}

