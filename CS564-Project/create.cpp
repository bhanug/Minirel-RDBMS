#include "catalog.h"


const Status RelCatalog::createRel(const string & relation, 
				   const int attrCnt,
				   const attrInfo attrList[])
{
  Status status;
  RelDesc rd;
  AttrDesc ad;

  if (relation.empty() || attrCnt < 1)
    return BADCATPARM;

  if (relation.length() >= sizeof rd.relName)
    return NAMETOOLONG;
  
  //Check whether a relation with the same name exists or not
  if(getInfo(relation, rd) != RELNOTFOUND){
    return RELEXISTS;
  }
  //Fill in the relation desc struct
  rd.attrCnt = attrCnt;
  memcpy(rd.relName, relation.c_str(), relation.length() + 1);
  //Add the relation
  Status addRelStatus = addInfo(rd);
  //If add relation fails, return
  if(addRelStatus != OK){
    return addRelStatus;
  }
  //For each attribute, add to attri catelog table
  int offset = 0;
  for(int i = 0; i < attrCnt; i ++){
    ad.attrLen = attrList[i].attrLen;
    memcpy(ad.attrName, attrList[i].attrName, MAXNAME);
    memcpy(ad.relName, relation.c_str(), relation.length()+1);
    ad.attrType = attrList[i].attrType;
    ad.attrOffset = offset;
    offset += ad.attrLen;
    Status addAttrStatus = attrCat->addInfo(ad);
    if(addAttrStatus != OK)
      return addAttrStatus;
  }
  //Create heap file to hold all tuples
  Status createFileStatus = createHeapFile(relation);
  if(createFileStatus != OK)
    return createFileStatus;
  return OK;
}

