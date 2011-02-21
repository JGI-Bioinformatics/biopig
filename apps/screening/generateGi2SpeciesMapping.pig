-- generateGi2SpeciesMapping.pig
--
-- given the gi taxid mapping file and the taxonomy to species mapping file,
-- creates an index to map gi to species.
--

%default p '50'
%default giindex '/users/kbhatia/SAG_Screening/gi_taxid_nucl.dmp'
%default names ' /users/kbhatia/SAG_Screening/names.dmp'
%default output '/users/kbhatia/gi2speciesname'

gi    = load '$giindex' as
          (gi: int, taxid: int);

names = load '$names' using
          PigStorage('|') as
          (taxid: int, speciesname: chararray,  junk: chararray, type: chararray);

filterednames = filter names by (type matches '\\s*scientific name\\s*');

x = join gi by taxid,
          filterednames by taxid PARALLEL $p;
y = foreach x generate gi::gi, filterednames::speciesname;

store y into '$output';