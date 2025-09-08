/*
********************************************************************************
  Author: Daniel Yupanqui
  First update (MM-DD-YY): 09-08-2025
  Last update  (MM-DD-YY): 09-08-2025
  Task: Simplifying the CCOFOG and Macroindicators do-file to streamline the
        import and processing of data stored in the CDUL-OUT Data section.		
*******************************************************************************/

*0. Set
{
  clear
  cd "\\cabinet.usask.ca\work$\ymn403\My Documents\Work\data_analysis\CSDUL\Building CSDUL\CDUL pilot - out and RDC\Node 1 - Political Context"
}

*1. Import Data
{
 *Import and harmonize files stored in GitHub	
  foreach x in gini_coefficient         ///
               unemployment_rate        ///
			   median_after_tax_income  ///
			   gdp                      ///
			   cpi                      ///
			   govt_expenditures {   

   *Import CSV
    import delimited data/`x'.csv, clear
	
   *Keep relevant variables
    keep geo ref_date value 				
  
   *Create values for provinces
    encode geo, gen(province) 				
    drop geo    

   *Renames	
    rename value `x' 					
	rename ref_date year 			
   
   *Save in dta
    save "data/`x'.dta", replace
}
}

*2. Processinf CCOFOG Data
{
 *Import CCOFOG folder */
  import delimited "data/ccofog_raw.csv", clear
	
 *Keep only necessary variables */
  keep geo ref_date value canadianclassificationoffunction
	
 *Destring province variable - geo */
  encode geo, gen(province)
  drop geo
	
 *Rename variable ref_date as 'year' */
  rename ref_date year
	
 /*Extract the digits contained within the square brackets in the 
   variable 'canadianClassificationOfFunction' */
  gen code = substr(canadianclassificationoffunction,                  ///
             strpos(canadianclassificationoffunction, "[")+ 1,         ///
			 strpos(canadianclassificationoffunction, "]") -           ///
			 strpos(canadianclassificationoffunction, "[") - 1)
	
 *Destring 
  encode code, gen(code_new)
  drop canadianclassificationoffunction code 
	
 *Reshape the dataset - from long to wide 
  reshape wide value, i(province year) j(code_new)
	
 *Renames 
  rename value1 	general_public_services_701  
 /*Executive and legislative organs, finacial 
   and fiscal affairs and external affairs*/ 
  rename value2 	gps_7011 
  rename value3 	foreign_economic_aid_7012
  rename value4 	general_services_7013
  rename value5 	basic_esearch_7014
  rename value6 	public_debt_transactions_7017

 *General public services not elsewhere classified [7015, 7016, 7018]  
  rename value7   gps_nec_7015_16_18 				
  rename value8 	defence_702
  rename value9 	military_defence_7021
  rename value10 	civil_defence_7022
  rename value11 	foreign_military_aid_7023
  
 *Defence not elsewhere classified [7024, 7025]  
  rename value12  defence_nec_7024_25					
  rename value13  public_order_and_safety_703
  rename value14  police_services_7031
  rename value15  fire_protection_services_7032
  rename value16  law_courts_7033
  rename value17  prisons_7034
  
 *Public order and safety not elsewhere classified [7035, 7036]  
  rename value18  pos_nec_7035_36					

 *General economic, commercial, and labor affairs 
  rename value19  economic_affairs_704
  rename value20  general_ecl_affairs_7041 	
 
 *Agriculture, foresty, fishing, and hunting 
  rename value21  agri_fore_fish_hunt_7042 		
  rename value22  fuel_and_energy_7043	
 
 *Mining, manufacturing, and construction 
  rename value23  mining_manu_construction_7044 	
  rename value24  transport_7045
 
 *Economic affairs not elsewhere classified [7046, 7047, 7048, 7049] 
  rename value25	economic_affairs_nec_7046to7049 	
  rename value26  environmental_protection_705
  rename value27  waste_management_7051
  rename value28  waster_water_management_7052
  rename value29  pollution_abatement_7053

 *Protection of biodiversity and landscape
  rename value30  protection_of_bandl_7054 		 
  
 *Environmental protection not elsewhere classified [7055, 7056] 
  rename value31  envir_protection_nec_7055_56 	
  
 *Housing and community development [7061, 7062]	
  rename value32  housing_and_commu_amenities_706
  rename value33  h_and_c_development_7061_62 	
  rename value34  water_supply_7063
  rename value35  street_lighting_7064
  
 *Housing and community amenities not elsewhere classified [7065, 7066]	
  rename value36  h_and_c_ammenities_nec_7065_66  
  rename value37  health_707
 *Medical products, appliances, and equipment [7071]
  rename value38  med_pae_7071 					
  rename value39  outpatient_services_7072
  rename value40  hospital_services_7073
  rename value41  public_health_services_7074
  
 *Health not elsewhere classified [7075, 7076] 
  rename value42  health_nec_7075_76 				

 *Recreation, culture and religion 
  rename value43  rcr_708
  
 *Recreational and sporting services
  rename value44  r_and_sporting_services_7081 	
  rename value45  cultural_services_7082

 *Broadcasting and publishing services	
  rename value46	b_and_p_services_7083 			
 
 *Recreation, culture, and religion not elsewhere classified [7084,7085,7086]
  rename value47  rcr_nec_7084to7086 				
  rename value48  education_709

 *Primary and secondary education
  rename value49  primary_and_secondary_edu_7092  
  rename value50	college_education_7093
  rename value51	university_education_7094

 *Education not elsewhere classified [7095, 7096, 7097, 7098]
  rename value52	education_nec_7095to7098 		
  rename value53	social_protection_710
  rename value54	sickness_and_disability_7101_03
  rename value55  old_age_7102
  rename value56	family_and_children_7104
  rename value57	unemployement_7105
  rename value58  housing_7106
  rename value59	social_exclusion_7107

 *Social protection not elsewhere classified [7108, 7109]
  rename value60	social_protection_nec_7108_09 	

 *Save the cleaned dataset in dta 
  save "data/ccofog.dta", replace							  
}	

*3. Merging Macroeconomic_indicators with CCOFOG
{
 *Import ccofog for merge
  use "data/ccofog.dta", clear
 
   foreach x in gini_coefficient         ///
               unemployment_rate        ///
			   median_after_tax_income  ///
			   gdp                      ///
			   cpi                      ///
			   govt_expenditures {  
    joinby year province using "data/`x'.dta", unm(b)
    tab _merge
	drop _merge
   }
	
 *Sort the dataset by province and year
  sort province year

 *Provinces and territories codes according to StatsCan classification
  decode province, generate(prov)
  drop province 
  generate province = 10 if prov ==	"Newfoundland and Labrador"
  replace  province = 11 if prov == "Prince Edward Island"
  replace  province = 12 if prov == "Nova Scotia"
  replace  province = 13 if prov == "New Brunswick"
  replace  province = 24 if prov == "Quebec"
  replace  province = 35 if prov == "Ontario"
  replace  province = 46 if prov == "Manitoba"
  replace  province = 47 if prov == "Saskatchewan"
  replace  province = 48 if prov == "Alberta"
  replace  province = 59 if prov == "British Columbia"
  replace  province = 60 if prov == "Yukon"
  replace  province = 61 if prov == "Northwest Territories"
  replace  province = 62 if prov == "Nunavut"

 *Drop prov 
  drop prov

 *Order
  order year province 
  
 *Save the final dataset in dta
  save "results/macroeconomic_indicators_and_ccofog.dta", replace 				
}

