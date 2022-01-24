/*Raw data*/
libname a "Data storage";

/*---Step1: Cleaning the Korean Adverse Event Reporting---*/

/*Step1-1: Keep the latest adverse event reports*/
/*Group table presents the information on initial and follow-up reports*/

data group1;
	set a.group;
	if group = . then group_num = kd_no; /* Missing value in a group variable means an initial report*/ 
	else group_num = group; 
	if seq = . then seq_num = 1; /*Missing value in a seq variable means an initial report*/
	else seq_num = seq;
	if trc_rpt_rsn_cd = . then trc_rpt_rsn_cd_num = 0; /*Missing value in a trc_rpt_rsn_cd means an initial report*/
	else trc_rpt_rsn_cd_num = trc_rpt_rsn_cd;
	drop group -- trs_rpt_rsn_cd_num;
run;

proc sql;
	create table latest_report as
	select distinct kd_no
	from (select distinct *, max(seq_num) as latest_report /*The maximum value of seq_num means the latest report*/
	      from group1
	      group by group_num) /*The packages for initial and follow-up reports are presented by group_num
				  ex) No. of initial report = 1111, No. of follow-up#1 report for that case = 2222, No. of follow-up#2 report for that case = 3333
				      This information is presented as
				      Initial report : Kd_no = 1111, group_num = 1111, seq = 1
			 	      Follow-up#1 report: Kd_no = 2222, group_num = 1111, seq = 2
			 	      Follow-up#2 report: Kd_no = 3333, group_num = 1111, seq = 3*/
	where seq_num = latest_report and trc_rpt_rsn_cd_num ^= 4 /*Keep the latest report*//*The value of 4 for trc_rpt_rsn_cd_num means a nullification of report*/  
	order by kd_no;
quit;

/*Step1-2: Delete the incorrect adverse event*/
/*drug_info_adr table presents the information on suspected or comcomitant drugs*/
/*adr_info_report table presents the information on reported adverse events*/

proc sql;
	create table drug_adverse_event_pair as
	select distinct *
	from (select distinct a.kd_no, a.doubt_cmbt_csf, a.drug_chem, a.dose_str_dt, b.whoart_arrn, b.whoart_seq, b.rvln_dt
	      from a.drug_info_adr as a inner join a.adr_info_report as b on a.kd_no = b.kd_no
	      where a.doubt_cmbt_csf = 1)
	      /*The value of 1 for doubt_cmbt_csf means the suspected drug*/
	where dose_str_dt <= rvln_dt
	/*dose_str_dt variable means the start date of medication*/
	/*rvln_dt variable means the start date of adverse events*/
	/*Delete reports when the start date of adverse events is earlier than that of the medication*/  
	order by kd_no;
quit;

/*Step1-3: Handle the missing value and variables in adr_report_basic and reportor_adr tables*/
/*adr_report_basic table presents information on charateristics of patients, reports, seriousness of adverse events*/
 
 data adr_report_basic;
 	set a.adr_report_basic;

	/*ptnt_sex variable is classied into 1 and 2*/
	/*The value of 1 and 2 in ptnt_sex means male and female, respectively*/
	if ptnt_sex = . then ptnt_sex_num = 0;
	else ptnt_sex_num = ptnt_sex;
	/*Missing value in a ptnt_sex variable means that the patient's sex is not reported*/
	/*The value of 0 for ptnt_sex_num means unknown*/
	
	/*In KAERS database, patient's age is presented by 3 variables, ptnt_occr_thtm_age, age_unit, and ptnt_agegp*/
	/*Age_unit variable is classified into 1, 2, 3, and 4.*/
	/*The value of 1,2,3, and 4 in age_unit means hours, days, months, and years, respectively*/
	/*ptnt_agegp variable is classified into 1,2,3,4,5 and 6*/
	/*The value of 1,2,3,4,5, and 6 in ptnt_agegp means neonate, infant, child, adolescent, adult, and elderly, respectively*/
	/*ptnt_occr_thtm_age variable presents the numeric value of patient's age at event.*/
	if age_unit = 3 then agg = 1; 
	else if age_unit =. and ptnt_agegp = " " then agg=0;
	else if age_unit =. and ptnt_agegp in (1,2,3,4) then agg=1;
	else if age_unit =. and ptnt_agegp in (5,6) then agg=0;
	else if age_unit = 4 and 0 <= ptnt_occr_thtm_age < 20 then agg=1;
	else if age_unit = 4 and 20 <= ptnt_occr_thtm_age < 30 then agg=2;
	else if age_unit = 4 and 30 <= ptnt_occr_thtm_age < 40 then agg=3;
	else if age_unit = 4 and 40 <= ptnt_occr_thtm_age < 50 then agg=4;
	else if age_unit = 4 and 50 <= ptnt_occr_thtm_age < 60 then agg=5;
	else if age_unit = 4 and 60 <= ptnt_occr_thtm_age < 70 then agg=6;
	else if age_unit = 4 and 70 <= ptnt_occr_thtm_age  then agg=7;
	/*agg presents "Age group"*/
	/*The value of 0 for agg means unknown*/

	/*rpt_csf variable presents the type of adverse event reports*/
	/*It is classified into 1,2,3, and 4*/
	/*The value of 1,2,3, and 4 in rpt_csf means that it is reported from post-marketing surveillance, clinical trials, 
	literatures, and other sources, respectively*/
	if rpt_csf =. then rpt_csf_num = 0;
	else rpt_csf_num = rpt_csf;
	/*Missing value in a rpt_csf variable means that the report type is not reported*/
	/*The value of 0 for rpt_csf_num means unknown*/

	keep kd_no year crtcl_case_yn rpt_csf_num ptnt_sex_num agg;
	/*Keep required variables*/
run;

/*adr_report_basic table presents information on charateristics of reporters*/

data reportor_adr;
	set a.reportor_adr;

	/*qualy_csf variable presents the source by person of adverse event reports*/
	/*It is classified into 1,2,3,4,5,6, and 7.*/
	/*The value of 1,2,3,4,5,6, and 7 means that report is reported by physician, pharmacist, nurse, consumer, 
	other, other health professional, and lawyer, respectively*/
	if qualy_csf =. then qualy_csf_num=0;
	else if qualy_csf = 6 then qualy_csf_num = 5; 
	else if qualy_csf in (5,7) then qualy_csf_num = 6; 
	else qualy_csf_num = qualy_csf;
	/*Missing value in a qualy_csf variable means that the source by person is not reported*/
	/*The value of 0 for qualy_csf_num means unknown*/
	/*The value of 5 for qualy_csf_num presents an other health professional*/
	/*The value of 6 for qualy_csf_num presents reports collected from lawyers and other occupation*/ 

	/*qualy_csf_1 variable presents the source by affliation of adverse event reports*/
	/*It is classified into 1,2,3,4,5,6, and 7.*/
	/*The value of 1,2,3,4,5,6, and 7 means that report is reported by regional pharmacovigilance center
	, manufacturer, medical institution, pharmacy, public health, consumer, and other, respectively*/ 
	if qualy_csf_1 = . then qualy_csf_1_num = 0;
	if qualy_csf_1 in (3,5) then qualy_csf_1_num = 3;
	else if qualy_csf_1 = 6 then qualy_csf_1_num=5;
	else if qualy_csf_1 = 7 then qualy_csf_1_num=6;
	else qualy_csf_1_num = qualy_csf_1;
	/*Missing value in a qualy_csf_1 variable means that the source by affiliation is not reported*/
	/*The value of 0 for qualy_csf_1_num means unknown*/
	/*The value of 3 for qualy_csf_1_num presents reports collected from medical institution and public health*/
	/*The value of 6 for qualy_csf_1_num presents reports collected from consumer*/ 
	/*The value of 7 for qualy_csf_1_num presents reports collected from other affiliation*/ 

	keep kd_no qualy_csf_num qualy_csf_1_num;
	/*Keep required variables*/
run;

/*Step2: Generate the database for analysis*/

/*Merge required tables*/

proc sql;
	create table analysis_DB as
	select distinct a.kd_no, b.drug_chem, b.whoart_arrn, b.whoart_seq, c.*, d.*
	from latest_report as a inner join drug_adverse_event_pair as b on a.kd_no=b.kd_no
				inner join adr_report_basic as c on a.kd_no = c.kd_no
				inner join reportor_adr as d on a.kd_no = d.kd_no
	order by a.kd_no;
quit; 


/*Keep information on study drug (Infliximab) and comparator (Methotrexate)*/

data analysis_study;
	set analysis_DB;
	if substr(drug_chem,1,10) = 'infliximab' or substr(drug_chem,1,12) = 'methotrexate';
	if substr(drug_chem,1,10) = 'infliximab' then study_drug = 1;
	else if substr(drug_chem,1,12) = 'methotrexate' then study_drug = 0;
	if 2010 <= year <= 2020; /*Set the study period*/
run;


/*Step3: Generate the database for implementing machine learning algorithms*/
/*Step3-1: Statistical feature*/

proc sql;
	create table stat_feature_preprocessing as
	select distinct a.*, b.c, b.d, max(m) as m1
	from (select distinct whoart_arrn, a, n-a as b
			  from (select distinct whoart_arrn, a, sum(a) as n
			  			from (select distinct whoart_arrn, count(kd_no) as a
								 from analysis_study
								 where study_drug = 1
								 group by whoart_arrn))) as a
			left join (select distinct whoart_arrn, c, m-c as d, m
						 from (select distinct whoart_arrn, c, sum(c) as m
						 		  from (select distinct whoart_arrn, count(kd_no) as c 
										   from analysis_study
								  		   where study_drug = 0
								  		   group by whoart_arrn))) as b
			on a.whoart_arrn = b.whoart_arrn
	order by a desc;
quit;
/*Variable a means the number of the targeted adverse event reports in the presence of the study drug.*/
/*Variable b means the number of the the other adverse event reports in the presence of the study drug.*/
/*Variable c means the number of the targeted adverse event reports in the presence of the comparator.*/
/*Variable d means the number of the the other adverse event reports in the presence of the comparator.*/

/* Handle missing c and d value*/
data statistical_feature;
	set stat_feature_preprocessing;
	if c=. then c = 0;
	if d=. then d = m1;
	drop m1;
run;

/*Step3-2: System organ feature*/
/*Use the WHO-ART ver0.92  for generating system organ feature*/ 
proc import out = system_organ_feature
	datafile = 'Data storage'
	DBMS = xlsx replace;
run;

/*Step3-3: Covariate feature*/
%macro covariate (var,group);
		if &var. = &group. then &var.&group. = 1; else &var.&group. =0;
%mend;

data covariate_feature_preprocessing;
	set analysis_study;

	if study_drug = 1;

	%covariate(agg,0);
	%covariate(agg,1);
	%covariate(agg,2);
	%covariate(agg,3);
	%covariate(agg,4);
	%covariate(agg,5);
	%covariate(agg,6);
	%covariate(agg,7);

	%covariate(ptnt_sex_num,0);
	%covariate(ptnt_sex_num,1);
	%covariate(ptnt_sex_num,2);

	if crtcl_case_yn = 'N' then crtcl_case_yn0 = 1;
	else crtcl_case_yn0 = 0;
	if crtcl_case_yn = 'Y' then crtcl_case_yn1 = 1;
	else crtcl_case_yn1 = 0;
	/*crtcl_case_yn presents whether reported adverse event is serious or not*/
	/*The value of 'Y' for crtcl_case_yn means that reported adverse event is serious*/
	/*The value of 'N' for crtcl_case_yn means that reported adverse event is serious*/

	%covariate(qualy_csf_num,0);
	%covariate(qualy_csf_num,1);
	%covariate(qualy_csf_num,2);
	%covariate(qualy_csf_num,3);
	%covariate(qualy_csf_num,4);
	%covariate(qualy_csf_num,5);
	%covariate(qualy_csf_num,6);

	%covariate(qualy_csf_1_num,0);
	%covariate(qualy_csf_1_num,1);
	%covariate(qualy_csf_1_num,2);
	%covariate(qualy_csf_1_num,3);
	%covariate(qualy_csf_1_num,4);
	%covariate(qualy_csf_1_num,5);
	%covariate(qualy_csf_1_num,6);

	drop year -- qualy_csf_1_num;
run;


proc sql;
	create table covariate_feature as
	select distinct whoart_arrn, 
			sum(agg0) as unknown_agegroup, sum(agg1) as agegroup_under_20, sum(agg2) as agegroup_20_29, sum(agg3) as agegroup_30_39, sum(agg4) as agegroup_40_49,
			sum(agg5) as agegroup_50_59, sum(agg5) as agegroup_60_69, sum(agg5) as agegroup_above_70,
			sum(ptnt_sex_num0) as unknown_sex, sum(ptnt_sex_num1) as male, sum(ptnt_sex_num2) as female,
			sum(crtcl_case_yn0) as non_serious, sum(crtcl_case_yn1) as serious,
			sum(qualy_csf_num0) as unknown_occupation, sum(qualy_csf_num1) as physician, sum(qualy_csf_num2) as pharmacist, sum(qualy_csf_num3) as nurse, sum(qualy_csf_num4) as consumer,
			sum(qualy_csf_num5) as other_health_professional, sum(qualy_csf_num6) as other_occupation,
			sum(qualy_csf_1_num0) as unknown_affiliation, sum(qualy_csf_1_num1) as RPVC, sum(qualy_csf_1_num2) as manufacturer, sum(qualy_csf_1_num3) as medical_institution,
			sum(qualy_csf_1_num4) as pharmacy, sum(qualy_csf_1_num5) as consumer, sum(qualy_csf_1_num6) as other_affiliation
	from covariate_feature_preprocessing
	group by whoart_arrn;
quit;

/*Step3-4: Label data*/
/*Load the constructed label data*/

proc import out = label
	datafile = 'Data storage'
	DBMS = xlsx replace;
	sheet = 'infliximab';
run;

/*Step3-5: Generating training dataset and prediction dataset*/

proc sql;
	create table training_dataset as
	select distinct a.whoart_arrn, a.label, b.*, c.soc1, d.*
	from label as a inner join statistical_feature as b on a.whoart_arrn = b.whoart_arrn
			inner join system_organ_feature as c on a.whoart_arrn = c.arrn_1
			inner join covariate_feature as d on a.whoart_arrn = d.whoart_arrn
	where a.label ^= 2
	order b.a desc;

	create table prediction_dataset as
	select distinct a.whoart_arrn, a.label, b.*, c.soc1, d.*
	from label as a inner join statistical_feature as b on a.whoart_arrn = b.whoart_arrn
			inner join system_organ_feature as c on a.whoart_arrn = c.arrn
			inner join covariate_feature as d on a.whoart_arrn = d.whoart_arrn
	where a.label = 2
	order b.a desc;
quit;
/*The value of 0 for label presents adverse event-pairs of which causality is definitely unrelated*/
/*The value of 1 for label presents adverse event-pairs of which causality is definitely related*/
/*The value of 2 for label presents adverse event-pairs of which causality is not fully uncovered*/



	

	




	

	








