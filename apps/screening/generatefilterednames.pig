

gi = load '/users/kbhatia/SAG_Screening/gi_taxid_nucl.dmp' as (gi: int, taxid: int);  
names = load '/users/kbhatia/SAG_Screening/names.dmp' USING PigStorage('|') AS (taxid: int, speciesname: chararray,  junk: chararray, type: chararray);
filterednames = filter names by (type matches '\\s*scientific name\\s*');

x = join gi by taxid, filterednames by taxid PARALLEL 100;
y = foreach x generate gi::gi, filterednames::speciesname;

store y into '/users/kbhatia/gi2speciesname';


