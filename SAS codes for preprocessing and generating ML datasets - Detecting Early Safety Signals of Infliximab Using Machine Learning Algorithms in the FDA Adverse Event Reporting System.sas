/* FAERS database is comprised of DEMO, DRUG, INDI, OUTC, REAC, and RPSR tables */

/* Load FAERS database in SAS system */

%macro DEMO;

	%do year = first_year %to last_year; 

	%do quarter = 1 %to 4;

		/* Load DEMO tables of AE reports in FAERS from first_year Q1 to last_year Q4*/
		proc import out = DEMO_&year.Q&quarter.
    		datafile = "Data storage location\DEMO&year.Q&quarter..txt"
        	dbms = dlm replace;	
        	getnames = yes;            	
        	delimiter = "$";            
    	run;

		/* Set "wt" variable to numeric */
		/* Inconsitent with "wt" variable across different DEMO data */
		data DEMO_&year.Q&quarter.;
			set DEMO_&year.Q&quarter.;
			wt_1 = wt*1;
			drop wt;
		run;

	%end; 

	/* Create yearly DEMO tables of AE reports*/
		data demo_&year.;
			set demo_&year.Q1 - demo_&year.Q4;
		run;
	
	%end;

%mend;

%DEMO;

/* Create the DEMO table during study period*/

data demo;
	set demo_first_year - demo_last_year.;
run;

/* Delete the duplicated report by using caseversion*/
/* Caseversion on each AE reports grouped by caseid*/
/* The later AE report has the higher caseversion*/
/* Leave only the latest AE reports*/

proc sql; 
	create table demo_&year._cum as
	select distinct primaryid, caseid, age, age_cod, sex, occp_cod
	from (select distinct *, max(caseversion) as a
	      from demo_&year._cum
	      group by caseid)
	where caseversion eq a
	order by caseid;
quit;


%macro DRUG;

	%do year = first_year %to last_year;
	%do quarter = 1 %to 4;

		/* Load Drug tables of AE reports in FAERS from first_year Q1 to last_year Q4*/
		proc import out = DRUG_&year.Q&quarter.
    		datafile = "Data storage location\DRUG&year.Q&quarter..txt"
        	dbms = dlm replace;	
        	getnames = yes;            	
        	delimiter = "$";            
    	run;

		data DRUG_&year.Q&quarter.;
			set DRUG_&year.Q&quarter.;
			keep primaryid caseid role_cod prod_ai; /*Keep only required variables*/
		run;

	%end;

		/* Create yearly Drug tables of AE reports*/
		data DRUG_&year.;
			set DRUG_&year.Q1 - DRUG_&year.Q4;
		run;
		
	%end;

%mend;

%DRUG;

/* Create the Drug table during study period*/

data DRUG;
	set DRUG_frist_year - DRUG_last_year;
	if role_cod in ('SS', 'PS'); 	/* Leave AE reports for "primary suspected drug" and "secondary suspected drug"*/
	if substr(prod_ai,1,10) = 'INFLIXIMAB' or substr(prod_ai,1,12) = 'METHOTREXATE'; /* Leave AE reports for study_drugs*/
run;

	

%macro OUTCOME;

	%do year = first_year %to last_year;
	%do quarter = 1 %to 4;

		/* Load Outcome tables of AE reports in FAERS from first_year Q1 to last_year Q4*/
		proc import out = OUTC_&year.Q&quarter.
    		datafile = "Data storage location\OUTC&year.Q&quarter..txt"
        	dbms = dlm replace;	
        	getnames = yes;            	
        	delimiter = "$";            
    	run;

	%end;

		/* Create yearly Outcome tables of AE reports*/
		data OUTC_&year.;
			set OUTC_&year.Q1 - OUTC_&year.Q4;
		run;
		
	%end;

%mend;

%OUTCOME;
 
/* Create the Outcome table during study period*/

data OUTC;
	set OUTC_first_year - OUTC_last_year;
run;



%macro REACTION;

	%do year = first_year %to last_year; 
	%do quarter = 1 %to 4;

		/* Load Reaction tables of AE reports in FAERS from first_year Q1 to last_year Q4*/
		proc import out = REAC_&year.Q&quarter.
    		datafile = "Data storage location\REAC&year.Q&quarter..txt"
        	dbms = dlm replace;	
        	getnames = yes;            	
        	delimiter = "$";            
    	run;

	%end; 

		/* Create yearly Reaction tables of AE reports*/
		data REAC_&year.;
			set REAC_&year.Q1 - REAC_&year.Q4;
		run;
	%end;

%mend;

%REACTION;

/* Create the Reaction table during study period*/

data REAC;
	set REAC_first_year - REAC_last_year.;
	keep primaryid caseid pt; /* Keep the required variables*/
run;




%macro RPSR;

	%do year = first_year %to last_year; 
	%do quarter = 1 %to 4;

		/* Load Reportsource tables of AE reports in FAERS from first_year Q1 to last_year Q4*/
		proc import out = RPSR_&year.Q&quarter.
    		datafile = "Data storage location\RPSR&year.Q&quarter..txt"
        	dbms = dlm replace;	
        	getnames = yes;            	
        	delimiter = "$";            
    	run;

	%end;

		/* Create yearly Reportsource tables of AE reports*/
		data RPSR_&year.;
			set RPSR_&year.Q1 - RPSR_&year.Q4;
		run;

	%end;

%mend;

%RPSR;


/* Create the Reportsource table during study period*/
data RPSR;
	set RPSR_first_year - RPSR_last_year.;
run;



/* Create feature data from FAERS database for implementing machine learning algorithms */
/*merge tables: DEMO, Drug, Reaction, Outcome, Reportsource*/

proc sql;
	create table preprocessing as
	select distinct a.*, b.*, c.*, d.*, e.*
	from demo as a inner join drug as b on a.primaryid = b.primaryid AND a.caseid = b.caseid
		       left join reac as c on a.primaryid = c.primaryid AND a.caseid = c.caseid
		       left join outc as d on a.primaryid = d.primaryid AND a.caseid = d.caseid
		       left join rpsr as e on a.primaryid = e.primaryid AND a.caseid = e.caseid
	order by primaryid;
quit;


/*Generate data for implementing machine learning*/

data preprocessing;
	set preprocessing;

	/*Assign 1 for study drug and 2 for comparator*/
	if substr(prod_ai,1,10) = 'INFLIXIMAB' then study_drug = 1;
	else study_drug = 2; 

	/*Assign age group code*/
	if age=. or age_cod = ' ' then agg = 0; /*0 is unknown*/
	else if age_cod ^= 'YR' then agg = 1;
	else if age < 20 then agg = 1;
	else if 20 <= age < 30 then agg=2;
	else if 30 <= age < 40 then agg=3;
	else if 40 <= age < 50 then agg=4;
	else if 50 <= age < 60 then agg=5;
	else if 60 <= age < 70 then agg=6;
	else agg=7; 

	/*Assign sex code*/
	if sex = 'M' then sex1 = 1;
	else if sex = 'F' then sex1= 2;
	else sex1=0; /*0 is unknown*/

	/*Assign occupation code*/
	if occp_cod = 'MD' then occu = 1;
	else if occp_cod = 'PH' then occu = 2;
	else if occp_cod = 'CN' then occu = 3;
	else if occp_cod in ('OT', 'LW')  then occu = 4;
	else occu = 0; /*0 is unknown*/
	
	/*Assign serious adverse event Y/N code*/
	if outc_cod = ' ' then SAE = 0; /*0 is non-serious adverse event*/
	else SAE =1;

	/*Assign reportsource code*/
	if rpsr_cod = ' ' then rpsr = 0; /*0 is unknown*/
	else if rpsr_cod = 'FGN' then rpsr = 1;
	else if rpsr_cod = 'SPY' then rpsr = 2;
	else if rpsr_cod = 'LIT' then rpsr = 3;
	else if rpsr_cod = 'CSM' then rpsr = 4;
	else if rpsr_cod = 'HP' then rpsr = 5;
	else if rpsr_cod = 'UF' then rpsr = 6;
	else if rpsr_cod = 'CR' then rpsr = 7;
	else if rpsr_cod = 'DT' then rpsr = 8;
	else rpsr = 9;
	
	drop age age_cod sex occp_cod outc_cod rpsr_cod; /*Keep the required variable*/

run;

/* Generate statistical feature */

proc sql;
	create table infliximab_contingency as
	select distinct pt, a, sum(a) as m
	from (select distinct pt, count(caseid) as a
	      from preprocessing
	      where study_drug eq 1
	      group by pt)
	order by a desc;

	create table methotrexate_contingency as
	select distinct pt, c, sum(c) as n
	from (select distinct pt, count(caseid) as c
	      from preprocessing
	      where study_drug eq 2
	      group by pt);

	create table contingency as
	select distinct  pt, a, m, c, max(n) as p
	from (select distinct a.*, b.*
	      from infliximab_contingency as a left join methotrexate_contingency as b on a.pt = b.pt)
	order by a desc;
quit;

	
data infliximab_statistical;
	set contingency;
	if c =. then c=0;
	
	b = m - a;
	d = p - c;

	keep pt a b c d;

run;

data infliximab_statistical;
	retain pt a b c d;
	set infliximab_statistical;
run;

/* Generate covariate feature */
%covariate (var,group);

	if &var. = &group. then &var.&group. = 1; else &var.&group. =0;

%mend;


data infliximab_covariate;
	set preprocessing;

	%covariate(agg,0);
	%covariate(agg,1);
	%covariate(agg,2);
	%covariate(agg,3);
	%covariate(agg,4);
	%covariate(agg,5);
	%covariate(agg,6);
	%covariate(agg,7);

	%covariate(sex1,0);
	%covariate(sex1,1);
	%covariate(sex1,2);

	%covariate(occu,0);
	%covariate(occu,1);
	%covariate(occu,2);
	%covariate(occu,3);
	%covariate(occu,4);

	%covariate(rpsr,0);
	%covariate(rpsr,1);
	%covariate(rpsr,2);
	%covariate(rpsr,3);
	%covariate(rpsr,4);
	%covariate(rpsr,5);
	%covariate(rpsr,6);
	%covariate(rpsr,7);
	%covariate(rpsr,8);
	%covariate(rpsr,9);

	%covariate(SAE,1);
	%covariate(SAE,0);

run;

proc sql;
	create table infliximab_covariate as
	select distinct pt, 
			sum(SAE1) as SAE_Y, sum(SAE0) as SAE_N,
			sum(sex10) as sex_unknown, sum(sex11) as male, sum(sex12) as female1, 
			sum(agg0) as age_unknown, sum(agg1) as age_group1, sum(agg2) as age_group2, sum(agg3) as age_group3, sum(agg4) as age_group4, sum(agg5) as age_group5, sum(agg6) as age_group6, sum(agg7) as age_group7,
			sum(occu0) as occu_unknown, sum(occu1) as physician, sum(occu2) as pharmacist, sum(occu3) as consumer_occu, sum(occu4) as HCP_occu,
			sum(rpsr0) as rpsr_unknown, sum(rpsr1) as foreign,  sum(rpsr2) as study, sum(rpsr3) as literature, sum(rpsr4) as consumer_rpsr, sum(rpsr5) as HCP_rpsr, sum(rpsr6) as User_facility, sum(rpsr7) as manufacturer, sum(rpsr8) as distributor, sum(rpsr9) as others_rpsr
	from infliximab_covariate
	where study_drug eq 1
	group by pt;
quit;

/* Merge statistical feature table and covariate feature table*/
proc sql;
	create table infliximab_feature as
	select distinct a.*, b.*
	from infliximab_statistical as a left join infliximab_covariate as b
	on a.pt = b.pt
	order by a desc;
quit;



/* Export to data in excel*/
proc export data=infliximab_feature
outfile =  "Data storage location\file name"
DBMS = xlsx replace;
run;
