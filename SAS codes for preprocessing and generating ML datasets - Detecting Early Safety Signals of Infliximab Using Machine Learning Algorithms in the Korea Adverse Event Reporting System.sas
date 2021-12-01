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
		data DEMO_&year.Q&ds.;
			set DEMO_&year.Q&ds.;
			wt_1 = wt*1;
			drop wt;
		run;

	%end; 

	/* Create yearly DEMO tables of AE reports*/
		data demo_&year.;
			set demo_first_year.Q1 - demo_&year.Q4;
		run;

	/* Create yearly cumulative DEMO tables of AE reports*/
		data demo_&year._cum;
			set demo_first_year - demo_&year.;
		run;

	/* Delete the duplicated report by using caseversion*/
	/* Caseversion on each AE reports grouped by caseid*/
	/* The later AE report has the higher caseversion*/
	/* Leave only the latest AE reports*/
		proc sql; 

			create table a.demo&year._cum as
			select distinct primaryid, caseid, age, age_cod, sex, occp_cod
			from (select distinct *, max(caseversion) as a
					from a.demo_&year._cum
					group by caseid)
			where caseversion eq a
			order by caseid;

		quit;

	%end;

%mend;

%DEMO;


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
			set DRUG_first_year&year.Q1 - DRUG_&year.Q4;
		run;

		/* Create yearly Drug tables of AE reports*/
		data DRUG_&year._cum;
			set DRUG_frist_year - DRUG_&year.;
			if role_cod in ('SS', 'PS'); 	/* Leave AE reports for "primary suspected drug" and "secondary suspected drug"*/
			if substr(prod_ai,1,10) = 'INFLIXIMAB' or substr(prod_ai,1,12) = 'METHOTREXATE'; /* Leave AE reports for study_drugs*/
		run;

	%end;

%mend;

%DRUG;

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
			set OUTC_&year.Q1 - OUTC&year.Q4;
		run;
 
		/* Create yearly cummulative Outcome tables of AE reports*/
		data OUTC_&year._cum;
			set OUTC_first_year - OUTC_&year.;
		run;

	%end;

%mend;

%OUTCOME;


%macro REACT;

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

		/* Create yearly cummulative Reaction tables of AE reports*/
		data REAC_&year._cum;
			set REAC_first_year - REAC_&year.;
			keep primaryid caseid pt; /* Keep the required variables*/
		run;

	%end;

%mend;

%REACT;


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

		/* Create yearly cummulative Reportsource tables of AE reports*/
		data RPSR_&year._cum;
			set RPSR_first_year - RPSR_&year.;
		run;

	%end;

%mend;

%RPSR;



/* Create feature data from FAERS database for implementing machine learning algorithms */
%macro preprocessing;

	%do year = first_year %to last_year;

	/*merge tables: DEMO, Drug, Reaction, Outcome, Reportsource*/
	proc sql;

		create table preprocessing_&year._cum as
		select distinct a.*, b.*, c.*, d.*, e.*
		from demo_&year._cum as a inner join drug_&year._cum as b on a0.primaryid = b.primaryid AND a.caseid = b.caseid;
													      left join reac_&year._cum as c on a.primaryid = c.primaryid AND c.caseid = b.caseid;
														  left join outc&year._cum as d on a.primaryid = d.primaryid AND a.caseid = d.caseid;
														  left join rpsr&year._cum as e on a.primaryid = e.primaryid AND a.caseid = e.caseid
		order by primaryid;
		quit;

	/*Generate data for implementing machine learning*/
	data preprocessing_&year._cum;
		set preprocessing_&year._cum;

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

		create table infliximab_contingency_&year._cum as
		select distinct pt, a, sum(a) as m
		from (select distinct pt, count(caseid) as a
				from preprocessing_&year._cum
				where study_drug eq 1
				group by pt)
		order by a desc;

		create table methotrexate_contingency_&year._cum as
		select distinct pt, c, sum(c) as n
		from (select distinct pt, count(caseid) as c
				from preprocessing_&year._cum
				where study_drug eq 2
				group by pt);

		create table contingency_&year._cum as
		select distinct  a, m, c, max(n) as p
		from (select distinct a.*, b.*
				from infliximab_contingency as a left join methotrexate_contingency as b
				on a.pt = b.pt)
		order by a desc;

	quit;

	data infliximab_statistical_feature_&year._cum;
		set contingency_&year._cum;
		if c =. then c=0;
	
		b = m - a;
		d = p - c;

		keep pt a b c d;

	run;

	data infliximab_statistical_feature_&year._cum;
		retain pt a b c d;
		set infliximab_statistical_feature_&year._cum;
	run;


	/* Generate covariate feature */
	data infliximab_covariate_feature_&year._cum;
		set preprocessing&year._cum;

		if sex1 = 1 then male = 1; else male = 0;
		if sex1 = 2 then female = 1; else female = 0;
		if sex1 = 3 then sex_unknown = 1; else sex_unknown = 0; 

		if agg = 0 then age_unknown = 1; else age_unknown = 0;
		if agg = 1 then age1 = 1; else age1=0;
		if agg = 2 then age2 = 1; else age2 = 0;
		if agg = 3 then age3 = 1; else age3 = 0;
		if agg = 4 then age4 = 1; else age4 = 0;
		if agg = 5 then age5 = 1; else age5 = 0;
		if agg = 6 then age6 = 1; else age6 = 0;
		if agg = 7 then age7 = 1; else age7 = 0;

		if occu = 0 then occu_unknown = 1; else occu_unknown = 0;
		if occu = 1 then occu1 = 1; else occu1 = 0;
		if occu = 2 then occu2 = 1; else occu2 = 0;
		if occu = 3 then occu3 = 1; else occu3 = 0;
		if occu = 4 then occu4 = 1; else occu4 = 0;

		if rpsr = 0 then rpsr_unknown = 1; else rpsr_unknown = 0;	
		if rpsr = 1 then rpsr1 = 1; else rpsr1 = 0;
		if rpsr = 2 then rpsr2 = 1; else rpsr2 = 0;
		if rpsr = 3 then rpsr3 = 1; else rpsr3 = 0;
		if rpsr = 4 then rpsr4 = 1; else rpsr4 = 0;
		if rpsr = 5 then rpsr5 = 1; else rpsr5 = 0;
		if rpsr = 6 then rpsr6 = 1; else rpsr6 = 0;
		if rpsr = 7 then rpsr7 = 1; else rpsr7 = 0;
		if rpsr = 8 then rpsr8 = 1; else rpsr8 = 0;
		if rpsr = 9 then rpsr9 = 1; else rpsr9 = 0;

		if SAE = 1 then SAE_Y = 1; else SAE_Y = 0;
		if SAE = 0 then SAE_N = 1; else SAE_N = 0;

	run;

	proc sql;
	create table infliximab_covariate_feature_&year._cum as
	select distinct pt, 
					  sum(SAE_Y) as SAE_Y1, sum(SAE_N) as SAE_N1,
					  sum(sex_unknown) as sex_unknown1, sum(male) as male1, sum(female) as female1, 
					  sum(age_unknown) as age_unknown1, sum(age1) as age11, sum(age2) as age21, sum(age3) as age31,sum(age4) as age41, sum(age5) as age51, sum(age6) as age61, sum(age7) as age71,
					  sum(occu_unknown) as occu_unknown1, sum(occu1) as occu11, sum(occu2) as occu21, sum(occu3) as occu31, sum(occu4) as occu41,
					  sum(rpsr_unknown) as rpsr_unknown1, sum(rpsr1) as rpsr11,  sum(rpsr2) as rpsr21, sum(rpsr3) as rpsr31, sum(rpsr4) as rpsr41, sum(rpsr5) as rpsr51, sum(rpsr6) as rpsr61, sum(rpsr7) as rpsr71, sum(rpsr8) as rpsr81, sum(rpsr9) as rpsr91
	from infliximab_covariate_feature_&year._cum
	where study_drug eq 1
	group by pt;
	quit;


	/* Merge statistical feature table and covariate feature table*/
	proc sql;
		create table infliximab_feature_&year._cum as
		select distinct a.*, b.*
		from infliximab_statistical_feature_&year._cum as a left join infliximab_covariate_feature_&year._cum as b
				on a.pt = b.pt
		order by a desc;
	quit;


	/* Export to data in excel*/
	proc export data=infliximab_feature_&year._cum
	outfile =  "Data storage location\file name&year."
	DBMS = xlsx replace;
	run;

%end;
 
%mend;

%preprocessing;
