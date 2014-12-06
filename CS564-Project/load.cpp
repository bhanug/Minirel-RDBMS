#include <unistd.h>
#include <fcntl.h>
#include "catalog.h"
#include "utility.h"


//
// Loads a file of (binary) tuples from a standard file into the relation.
// Any indices on the relation are updated appropriately.
//
// Returns:
// 	OK on success
// 	an error code otherwise
//

const Status UT_Load(const string & relation, const string & fileName)
{
  cerr << "[UT-load] begin " << relation << " " << fileName << endl;
  Status status;
  RelDesc rd;
  AttrDesc *attrs;
  int attrCnt;
  InsertFileScan * iFile;
  int width = 0;

  if (relation.empty() || fileName.empty() || relation == string(RELCATNAME)
      || relation == string(ATTRCATNAME))
    return BADCATPARM;

  // open Unix data file

  int fd;
  if ((fd = open(fileName.c_str(), O_RDONLY, 0)) < 0)
    return UNIXERR;

  // get relation data
  status = attrCat->getRelInfo(relation, attrCnt, attrs);
  if(status != OK)
    return status;
  for(int i = 0; i < attrCnt; i ++){
    width += attrs[i].attrLen;
  }
  cerr << "[UT-load] total width " << width << endl;
  // start insertFileScan on relation
  iFile = new InsertFileScan(relation,status);
  if(status != OK)
    return status;


  // allocate buffer to hold record read from unix file
  char *record;
  if (!(record = new char [width])) return INSUFMEM;

  int records = 0;
  int nbytes;

  // read next input record from Unix file and insert it into relation
  while((nbytes = read(fd, record, width)) == width) {
    cerr << "[UT-load] width " << width << endl;

    RID rid;
    Record rec;
    rec.data = new char[width];
    memcpy((void*)rec.data, record, width);
    rec.length = width;
    cerr << "[UT-load] read #" << records << " " << rec.length << " " << width << endl;
    if ((status = iFile->insertRecord(rec, rid)) != OK) return status;
    records++;
  }
  cerr << "[UT-load] remain " << nbytes << endl;
  delete iFile;
  UT_Print(relation);
  // close heap file and unix file
  if (close(fd) < 0) return UNIXERR;
  cerr << "[UT-load] finished " << endl;
  return OK;
}

