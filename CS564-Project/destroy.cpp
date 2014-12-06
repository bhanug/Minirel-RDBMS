#include "catalog.h"

//
// Destroys a relation. It performs the following steps:
//
// 	removes the catalog entry for the relation
// 	destroys the heap file containing the tuples in the relation
//
// Returns:
// 	OK on success
// 	error code otherwise
//

const Status RelCatalog::destroyRel(const string & relation)
{
  Status status;

  if (relation.empty() || 
      relation == string(RELCATNAME) || 
      relation == string(ATTRCATNAME))
    return BADCATPARM;

  int attrCnt = 0;
  AttrDesc* attrDescList;
  //Get all attr desc entries
  attrCat->getRelInfo(relation, attrCnt, attrDescList);
  //Remove all of them
  for(int i = 0; i < attrCnt; i ++){
    string attrname(attrDescList[i].attrName);
     status = attrCat->removeInfo(relation, attrname);
    if(status != OK)
      return status;
  }
  //Remove relation from rel table
  status = removeInfo(relation);
  if(status != OK)  return status;
  //Remove heap file
  status = destroyHeapFile(relation);
  return status;
}


//
// Drops a relation. It performs the following steps:
//
// 	removes the catalog entries for the relation
//
// Returns:
// 	OK on success
// 	error code otherwise
//

const Status AttrCatalog::dropRelation(const string & relation)
{
  Status status;
  AttrDesc *attrs;
  int attrCnt, i;

  if (relation.empty()) return BADCATPARM;





}


