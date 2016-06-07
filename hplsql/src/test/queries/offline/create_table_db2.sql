 
------------------------------------------------
-- DDL Statements for Table  "ABCDEF_ABI"
------------------------------------------------ 
 
CREATE TABLE  "ABCDEF_ABI"  (
  "ABI_CCANT_POSTN_SK" INT NOT NULL ,
  "LAT_DATE" DATE NOT NULL ,
  "LAT_TIME" TIMESTAMP NOT NULL ,
  "LAT_ACTION" CHAR(2) NOT NULL ,
  "ROW_ID" VARCHAR(15) NOT NULL ,
  "CREATED" TIMESTAMP NOT NULL ,
  "CREATED_BY" VARCHAR(15) NOT NULL ,
  "LAST_UPD" TIMESTAMP NOT NULL ,
  "LAST_UPD_BY" VARCHAR(15) NOT NULL ,
  "MODIFICATION_NUM" DECIMAL(10,0) NOT NULL ,
  "ASGN_DNRM_FLG" CHAR(1) ,
  "ROLE_CD" VARCHAR(30) ,
  "STATUS" VARCHAR(30) ,
  "BD_LAST_UPD" TIMESTAMP ,
  "BD_LAST_UPD_SRC" VARCHAR(50) ,
  "ABCDEF_CNT" INT NOT NULL ,
  "ABCDEF_IND" CHAR(1) NOT NULL ,
  "ABCDEF_TYP_CD" CHAR(1) NOT NULL WITH DEFAULT  ,
  "PRE_LKO_MAPNG_ID" INT NOT NULL WITH DEFAULT 0 ,
  "CR_BY_MAPNG_ID" INT NOT NULL ,
  "DW_CR_TMSP" TIMESTAMP NOT NULL ,
  "WRK_FLOW_RUN_ID" INT NOT NULL )
COMPRESS YES
WITH RESTRICT ON DROP
DISTRIBUTE BY HASH("ABI_CCANT_POSTN_SK")
 IN "WFDTS2_16K" INDEX IN "WFDIX2_16K" ;

 ------------------------------------------------
-- DDL Statements for Table  "ABCDEF_ABI_ACT"
------------------------------------------------
 
 
CREATE TABLE  "ABCDEF_ABI_ACT"  (
  "ABI_ACT_CONTACT_SK" INT NOT NULL ,
  "LAT_DATE" DATE NOT NULL ,
  "LAT_TIME" TIMESTAMP NOT NULL ,
  "LAT_ACTION" CHAR(2) NOT NULL ,
  "ROW_ID" VARCHAR(15) NOT NULL ,
  "ABCDEF_IND" CHAR(1) NOT NULL ,
  "ABCDEF_TYP_CD" CHAR(1) NOT NULL WITH DEFAULT  ,
  "PRE_LKO_MAPNG_ID" INT NOT NULL WITH DEFAULT 0 ,
  "BD_LAST_UPD_SRC" VARCHAR(50) ,
  "UPD_BY_MAPNG_ID" INT NOT NULL ,
  "WRK_FLOW_RUN_ID" INT NOT NULL )
COMPRESS YES
WITH RESTRICT ON DROP
DISTRIBUTE BY HASH("ABI_ACT_CONTACT_SK")
IN "WFDTS2_16K" INDEX IN "WFDIX2_16K" ;
           
------------------------------------------------
-- DDL Statements for Table  "ABCDEF_ABI_EMP"
------------------------------------------------ 
 
CREATE TABLE  "ABCDEF_ABI_EMP"  (
  "ABI_ACT_EMP_SK" INT NOT NULL ,
  "LAT_DATE" DATE NOT NULL ,
  "LAT_TIME" TIMESTAMP NOT NULL ,
  "LAT_ACTION" CHAR(2) NOT NULL ,
  "ABCDEF_CNT" INT NOT NULL ,
  "ABCDEF_IND" CHAR(1) NOT NULL ,
  "ABCDEF_TYP_CD" CHAR(1) NOT NULL WITH DEFAULT  ,
  "PRE_LKO_MAPNG_ID" INT NOT NULL WITH DEFAULT 0 ,
  "DW_CR_TMSP" TIMESTAMP NOT NULL ,
  "CR_BY_MAPNG_ID" INT NOT NULL ,
  "DW_UPD_TMSP" TIMESTAMP NOT NULL ,
  "UPD_BY_MAPNG_ID" INT NOT NULL ,
  "WRK_FLOW_RUN_ID" INT NOT NULL )
COMPRESS YES
WITH RESTRICT ON DROP
DISTRIBUTE BY HASH("ABI_ACT_EMP_SK")
IN "WFDTS2_16K" INDEX IN "WFDIX2_16K" ;