package com.brindys.deTools.pslFeatureMapper;

import java.util.*;
import java.util.regex.*;

public class PSLFeatureMapper {

  private Map<String, Feature> features = new LinkedHashMap<>();
  private Map<String, Connection> connections = new LinkedHashMap<>();
  private List<Query> queries = new ArrayList<>();
  private Map<String, SchemaDefinition> schemas = new LinkedHashMap<>();
  private Map<String, String> variables = new HashMap<>();
  private String datasetName = "UNKNOWN";
  private String datasetVersion = "UNKNOWN";
  private List<Transform> transforms = new ArrayList<>();
  private List<Hierarchy> hierarchies = new ArrayList<>();

  public static void main(String[] args) {
    // Test with the Emory config
    String samplePSL = loadSampleConfig();
    PSLFeatureMapper mapper = new PSLFeatureMapper();
    mapper.parse(samplePSL);
    mapper.printLineage();
  }

  private static String loadSampleConfig() {
    // This would normally read from a file
    // For now, paste the config content here or pass as argument
    return "DATASET PHR_NVS {\n"
        + "PRECISION = DAY\n"
        + "COMPRESSION=TRUE\n"
        + "STATISTICS=FALSE\n"
        + "DATA_PROVIDER_CODE=PHR\n"
        + "DATA_SOURCE_CODE=NVS\n"
        + "DATASET_VERSION=v1.2.0\n"
        + "DATASET_EFFECTIVE_DATE=2025-06-30\n"
        + "DATASET_DESCRIPTION_JSON=description.json\n"
        + "DATASET_STATISTICS_JSON=statistics.json\n"
        + "FUTURE_CUTOFF=3 MONTHS\n"
        + "SOURCE_RX_CLAIMS=TRUE\n"
        + "SOURCE_MEDICAL_CLAIMS=TRUE\n"
        + "SOURCE_OMOP=FALSE\n"
        + "SOURCE_EHR=TRUE\n"
        + "\n"
        + "}\n"
        + "\n"
        + "#PROCEDURE\n"
        + "FEATURE CPT, CPT, STRING, INDEXED, SEARCHABLE, NULL\n"
        + "FEATURE PROCEDURE_DATE, PROCEDURE, DATE\n"
        + "FEATURE ICD10PCS, ICD10PCS, STRING, INDEXED, SEARCHABLE, NULL\n"
        + "\n"
        + "#VISIT\n"
        + "FEATURE VISIT_TYPE, VISIT TYPE, STRING, INDEXED, SEARCHABLE, NULL\n"
        + "FEATURE VISIT_DATE, DURATION OF VISIT, DATE_START_END\n"
        + "\n"
        + "#PERSON\n"
        + "FEATURE RACE, RACE, STRING, INDEXED, SEARCHABLE, NULL\n"
        + "FEATURE ETHNICITY, ETHNICITY, STRING, INDEXED, SEARCHABLE, NULL\n"
        + "FEATURE GENDER, GENDER, STRING, INDEXED, SEARCHABLE, NULL\n"
        + "FEATURE DEATH, DEATH, DATE, SEARCHABLE, NULL\n"
        + "FEATURE ZIP, ZIP CODE, STRING, SEARCHABLE, NULL\n"
        + "FEATURE GEOGRAPHIC_ENTITY, GEOGRAPHIC CODE, STRING, SEARCHABLE, NULL\n"
        + "FEATURE GEOGRAPHIC_ENTITY_TYPE, GEOGRAPHIC CODE TYPE, STRING, SEARCHABLE, NULL\n"
        + "\n"
        + "#DRUG_EXPOSURE\n"
        + "FEATURE RX, RX, STRING, INDEXED, SEARCHABLE, NULL\n"
        + "FEATURE ATC, ATC, STRING, INDEXED, SEARCHABLE, NULL\n"
        + "FEATURE DRUG_EXPOSURE_DATE, DATE MEDICATION WAS TAKEN, DATE_START_END\n"
        + "FEATURE ROUTE, ROUTE, STRING, SEARCHABLE, INDEXED, NULL\n"
        + "FEATURE DRUG_EXPOSURE_TYPE, DRUG EXPOSURE TYPE, STRING, INDEXED, SEARCHABLE, NULL\n"
        + "FEATURE NDC, NDC, STRING, INDEXED, SEARCHABLE, NULL\n"
        + "FEATURE EVENT_ID, EVENT ID, STRING, METALINK\n"
        + "\n"
        + "#CONDITION\n"
        + "FEATURE ICD9, ICD9, STRING, INDEXED, SEARCHABLE\n"
        + "FEATURE ICD10, ICD10, STRING, INDEXED, SEARCHABLE\n"
        + "FEATURE DIAGNOSIS_DATE, DIAGNOSIS DATE, DATE_START_END\n"
        + "\n"
        + "#MEASUREMENT\n"
        + "FEATURE VALUE_AS_STRING, CATEGORICAL VALUE, STRING, INDEXED, SEARCHABLE, NULL\n"
        + "FEATURE VALUE_AS_NUMBER, NUMERIC VALUE, DOUBLE\n"
        + "FEATURE LOINC, LOINC, STRING, INDEXED, SEARCHABLE, NULL\n"
        + "FEATURE UNIT, UNIT, STRING, SEARCHABLE, NULL\n"
        + "FEATURE MEASUREMENT_DATE, DATE LAB WAS TAKEN, DATE\n"
        + "\n"
        + "#PAYER_PLAN_PERIOD\n"
        + "FEATURE PAYER_PLAN_PERIOD_DATE, PAYER PLAN PERIOD DATE, DATE_START_END\n"
        + "FEATURE PAYER_TYPE, PAYER TYPE, STRING, INDEXED, SEARCHABLE, NULL\n"
        + "FEATURE PAYER, PAYER, STRING, INDEXED, SEARCHABLE, NULL\n"
        + "\n"
        + "#PROVIDER\n"
        + "FEATURE PROVIDER_ID, PROVIDER ID, STRING, METALINK, SEARCHABLE, INDEXED\n"
        + "FEATURE NPI, NPI, STRING, INDEXED, SEARCHABLE, NULL\n"
        + "FEATURE CARE_SITE, CARE SITE, STRING, INDEXED, SEARCHABLE, NULL\n"
        + "FEATURE PROVIDER_SPECIALTY, PROVIDER SPECIALTY, STRING, INDEXED, SEARCHABLE, NULL\n"
        + "FEATURE PROVIDER_TYPE, PROVIDER TYPE, STRING, INDEXED, SEARCHABLE, NULL\n"
        + "\n"
        + "\n"
        + "VARIABLE AND_PERSON_SUB {AND person_id >= 0}\n"
        + "VARIABLE PERSON_SUB {WHERE person_id >= 0}\n"
        + "VARIABLE ALIAS_PERSON_SUB {WHERE p.person_id >= 0}\n"
        + "VARIABLE LIMIT { }\n"
        + "VARIABLE DATA_SCHEMA {unifyplus_eu_atropos_poc.atropos_pre_transformed}\n"
        + "VARIABLE VOCAB_SCHEMA {unifyplus_eu_atropos_poc.atropos_pre_transformed}\n"
        + "\n"
        + "\n"
        + "\n"
        + "#----------------------------DB CONNECTION-----------------------------------------\n"
        + "\n"
        + "CONNECTION OPTUM {\n"
        + "         url=jdbc:2\n"
        + "\n"
        + "        }\n"
        + "\n"
        + "\n"
        + "\n"
        + "#-----------------------------PERSON-----------------------------\n"
        + "\n"
        + "PATIENT LEVEL SCHEMA PERSON {\n"
        + "  GENDER\n"
        + "  RACE\n"
        + "  ETHNICITY\n"
        + "  DEATH\n"
        + "  ZIP\n"
        + "  GEOGRAPHIC_ENTITY\n"
        + "  GEOGRAPHIC_ENTITY_TYPE\n"
        + "}\n"
        + "\n"
        + "CONNECTION PERSON FROM OPTUM {\n"
        + "    SORT COLUMN = *ptid*\n"
        + "    CACHE = person\n"
        + "    QUERY = SELECT\n"
        + "                ptid,\n"
        + "                region,\n"
        + "                CAST(birth_yr || '-01-01' AS DATE) AS dob,\n"
        + "                upper(gender) as gender,\n"
        + "                upper(race) as race,\n"
        + "                upper(ethnicity) as ethnicity,\n"
        + "                CAST(substr(date_of_death,1,4) || '-' || substr(date_of_death,5,6) || '-01' as DATE) as dod\n"
        + "            from $DATA_SCHEMA.optum_ehr_202508__pt_clinical\n"
        + "}\n"
        + "\n"
        + "QUERY PERSON FROM PERSON{\n"
        + "  PID = *ptid*\n"
        + "  GENDER = *gender*\n"
        + "  GENDER.NAME = *gender*\n"
        + "  DOB = *dob*\n"
        + "  RACE = *race*\n"
        + "  RACE.NAME = *race*\n"
        + "  ETHNICITY = *ethnicity*\n"
        + "  ETHNICITY.NAME = *ethnicity*\n"
        + "  DEATH = *dod*\n"
        + "  ZIP = NULL\n"
        + "  GEOGRAPHIC_ENTITY = *region*\n"
        + "  GEOGRAPHIC_ENTITY_TYPE = \"REGION\"\n"
        + "}\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "#--------------------------------ICD10-----------------------------------------\n"
        + "\n"
        + "CONNECTION CACHE_ICD10 FROM OPTUM {\n"
        + "    CACHE = icd10\n"
        + "    SORT COLUMN= *ptid*\n"
        + "    QUERY = SELECT\n"
        + "                d.ptid,\n"
        + "                d.diag_date,\n"
        + "                d.diagnosis_cd,\n"
        + "                e.provid\n"
        + "            from $DATA_SCHEMA.optum_ehr_202508__diag d\n"
        + "                LEFT JOIN (\n"
        + "            SELECT\n"
        + "                encid,\n"
        + "                provid,\n"
        + "                ROW_NUMBER() OVER (\n"
        + "                    PARTITION BY encid\n"
        + "                    ORDER BY\n"
        + "                        CASE\n"
        + "                            WHEN provider_role = 'ATTENDING' THEN 1\n"
        + "                            WHEN provider_role = 'ADMITTING' THEN 2\n"
        + "                            ELSE 3\n"
        + "                        END,\n"
        + "                        provid\n"
        + "                ) as rn\n"
        + "            FROM\n"
        + "                $DATA_SCHEMA.optum_ehr_202508__enc_prov\n"
        + "        ) e\n"
        + "        ON\n"
        + "            d.encid = e.encid AND e.rn = 1\n"
        + "        WHERE\n"
        + "    d.diagnosis_cd_type = 'ICD10'\n"
        + "}\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "DEDUPLICATED SCHEMA ICD10 {\n"
        + "    ICD10\n"
        + "    DIAGNOSIS_DATE\n"
        + "    PROVIDER_ID\n"
        + "}\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "QUERY ICD10 FROM CACHE_ICD10 {\n"
        + "    PID = *ptid*\n"
        + "    ICD10 = MAP(ICD10CODE, *diagnosis_cd*)\n"
        + "    DIAGNOSIS_DATE.START = *diag_date*\n"
        + "    DIAGNOSIS_DATE.END = *diag_date*\n"
        + "    PROVIDER_ID = *provid*\n"
        + "}\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "#--------------------------------ICD9-----------------------------------------\n"
        + "\n"
        + "CONNECTION CACHE_ICD9 FROM OPTUM {\n"
        + "    CACHE = icd9\n"
        + "    SORT COLUMN= *ptid*\n"
        + "    QUERY = SELECT\n"
        + "                d.ptid,\n"
        + "                d.diag_date,\n"
        + "                d.diagnosis_cd,\n"
        + "                e.provid\n"
        + "            from $DATA_SCHEMA.optum_ehr_202508__diag d\n"
        + "                LEFT JOIN (\n"
        + "            SELECT\n"
        + "                encid,\n"
        + "                provid,\n"
        + "                ROW_NUMBER() OVER (\n"
        + "                    PARTITION BY encid\n"
        + "                    ORDER BY\n"
        + "                        CASE\n"
        + "                            WHEN provider_role = 'ATTENDING' THEN 1\n"
        + "                            WHEN provider_role = 'ADMITTING' THEN 2\n"
        + "                            ELSE 3\n"
        + "                        END,\n"
        + "                        provid\n"
        + "                ) as rn\n"
        + "            FROM\n"
        + "                $DATA_SCHEMA.optum_ehr_202508__enc_prov\n"
        + "        ) e\n"
        + "        ON\n"
        + "            d.encid = e.encid AND e.rn = 1\n"
        + "        WHERE\n"
        + "    d.diagnosis_cd_type = 'ICD9'\n"
        + "\n"
        + "            }\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "DEDUPLICATED SCHEMA ICD9 {\n"
        + "    ICD9\n"
        + "    DIAGNOSIS_DATE\n"
        + "    PROVIDER_ID\n"
        + "}\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "QUERY ICD9 FROM CACHE_ICD9 {\n"
        + "    PID = *ptid*\n"
        + "    ICD9 = MAP(ICD9CODE, *diagnosis_cd*)\n"
        + "    DIAGNOSIS_DATE.START = *diag_date*\n"
        + "    DIAGNOSIS_DATE.END = *diag_date*\n"
        + "    PROVIDER_ID = *provid*\n"
        + "}\n"
        + "\n"
        + "#-----------------------------PROCEDURE_CPT--------------------------------------\n"
        + "\n"
        + "DEDUPLICATED SCHEMA CPT {\n"
        + "CPT\n"
        + "PROCEDURE_DATE\n"
        + "PROVIDER_ID\n"
        + "}\n"
        + "\n"
        + "CONNECTION CACHE_PROCEDURE_CPT FROM OPTUM {\n"
        + "CACHE = procedure_cpt\n"
        + "SORT COLUMN = *ptid*\n"
        + "QUERY = SELECT\n"
        + "            d.ptid,\n"
        + "            d.proc_date,\n"
        + "            d.proc_code,\n"
        + "            e.provid\n"
        + "        FROM\n"
        + "            $DATA_SCHEMA.optum_ehr_202508__proc d\n"
        + "        LEFT JOIN (\n"
        + "            SELECT\n"
        + "                encid,\n"
        + "                provid,\n"
        + "                ROW_NUMBER() OVER (\n"
        + "                    PARTITION BY encid\n"
        + "                    ORDER BY\n"
        + "                        CASE\n"
        + "                            WHEN provider_role = 'ATTENDING' THEN 1\n"
        + "                            WHEN provider_role = 'ADMITTING' THEN 2\n"
        + "                            ELSE 3\n"
        + "                        END,\n"
        + "                        provid\n"
        + "                ) as rn\n"
        + "            FROM\n"
        + "                $DATA_SCHEMA.optum_ehr_202508__enc_prov\n"
        + "        ) e\n"
        + "        ON\n"
        + "            d.encid = e.encid AND e.rn = 1\n"
        + "        WHERE\n"
        + "    d.proc_code_type = 'CPT4' OR d.proc_code_type = 'HCPCS'\n"
        + "}\n"
        + "\n"
        + "\n"
        + "QUERY CPT FROM CACHE_PROCEDURE_CPT {\n"
        + "PID = *ptid*\n"
        + "CPT= *proc_code*\n"
        + "PROCEDURE_DATE = *proc_date*\n"
        + "PROVIDER_ID = *provid*\n"
        + "}\n"
        + "\n"
        + "#---------------------------- PROCEDURE_ICD10PCS----------------------------------------\n"
        + "\n"
        + "DEDUPLICATED SCHEMA ICD10PCS {\n"
        + "ICD10PCS\n"
        + "PROCEDURE_DATE\n"
        + "PROVIDER_ID\n"
        + "}\n"
        + "\n"
        + "CONNECTION CACHE_PROCEDURE_ICD10PCS FROM OPTUM {\n"
        + "CACHE = procedure_icd10pcs\n"
        + "SORT COLUMN = *ptid*\n"
        + "QUERY = SELECT\n"
        + "            d.ptid,\n"
        + "            d.proc_date,\n"
        + "            d.proc_code,\n"
        + "            e.provid\n"
        + "        FROM\n"
        + "            $DATA_SCHEMA.optum_ehr_202508__proc d\n"
        + "        LEFT JOIN (\n"
        + "            SELECT\n"
        + "                encid,\n"
        + "                provid,\n"
        + "                ROW_NUMBER() OVER (\n"
        + "                    PARTITION BY encid\n"
        + "                    ORDER BY\n"
        + "                        CASE\n"
        + "                            WHEN provider_role = 'ATTENDING' THEN 1\n"
        + "                            WHEN provider_role = 'ADMITTING' THEN 2\n"
        + "                            ELSE 3\n"
        + "                        END,\n"
        + "                        provid\n"
        + "                ) as rn\n"
        + "            FROM\n"
        + "                $DATA_SCHEMA.optum_ehr_202508__enc_prov\n"
        + "        ) e\n"
        + "        ON\n"
        + "            d.encid = e.encid AND e.rn = 1\n"
        + "        WHERE\n"
        + "    d.proc_code_type = 'ICD10'\n"
        + "\n"
        + "}\n"
        + "\n"
        + "QUERY ICD10PCS FROM CACHE_PROCEDURE_ICD10PCS {\n"
        + "PID = *ptid*\n"
        + "ICD10PCS= *proc_code*\n"
        + "PROCEDURE_DATE = *proc_date*\n"
        + "PROVIDER_ID = *provid*\n"
        + "}\n"
        + "\n"
        + "\n"
        + "#---------------------------- PROCEDURE_ICD9Proc----------------------------------------\n"
        + "\n"
        + "\n"
        + "CONNECTION CACHE_PROCEDURE_ICD9Proc FROM OPTUM {\n"
        + "CACHE = procedure_icd9proc\n"
        + "SORT COLUMN = *ptid*\n"
        + "QUERY = SELECT\n"
        + "            d.ptid,\n"
        + "            d.proc_date,\n"
        + "            d.proc_code,\n"
        + "            e.provid\n"
        + "        FROM\n"
        + "            $DATA_SCHEMA.optum_ehr_202508__proc d\n"
        + "        LEFT JOIN (\n"
        + "            SELECT\n"
        + "                encid,\n"
        + "                provid,\n"
        + "                ROW_NUMBER() OVER (\n"
        + "                    PARTITION BY encid\n"
        + "                    ORDER BY\n"
        + "                        CASE\n"
        + "                            WHEN provider_role = 'ATTENDING' THEN 1\n"
        + "                            WHEN provider_role = 'ADMITTING' THEN 2\n"
        + "                            ELSE 3\n"
        + "                        END,\n"
        + "                        provid\n"
        + "                ) as rn\n"
        + "            FROM\n"
        + "                $DATA_SCHEMA.optum_ehr_202508__enc_prov\n"
        + "        ) e\n"
        + "        ON\n"
        + "            d.encid = e.encid AND e.rn = 1\n"
        + "        WHERE\n"
        + "    d.proc_code_type = 'ICD9'\n"
        + "\n"
        + "}\n"
        + "\n"
        + "QUERY ICD9 FROM CACHE_PROCEDURE_ICD9Proc {\n"
        + "PID = *ptid*\n"
        + "ICD9 = MAP(ICD9CODE, *proc_code*)\n"
        + "DIAGNOSIS_DATE.START= *proc_date*\n"
        + "DIAGNOSIS_DATE.END = *proc_date*\n"
        + "PROVIDER_ID = *provid*\n"
        + "}\n"
        + "\n"
        + "#---------------------------------MEASUREMENT----------------------------------------\n"
        + "\n"
        + "\n"
        + "CONNECTION CACHE_LOINC1 FROM OPTUM {\n"
        + "CACHE = loincs1\n"
        + "SORT COLUMN = *ptid*\n"
        + "QUERY = SELECT\n"
        + "            ptid,\n"
        + "            test_result,\n"
        + "            loinc,\n"
        + "            COALESCE(collected_date, result_date) AS primary_date,\n"
        + "            result_unit,\n"
        + "            result_datatype\n"
        + "       FROM $DATA_SCHEMA.optum_ehr_202508__lab where result_datatype is not null and result_date <= '2016-01-01'\n"
        + "\n"
        + "}\n"
        + "\n"
        + "\n"
        + "\n"
        + "SCHEMA MEASUREMENT {\n"
        + "    LOINC\n"
        + "    MEASUREMENT_DATE\n"
        + "    VALUE_AS_NUMBER\n"
        + "    VALUE_AS_STRING\n"
        + "    UNIT\n"
        + "}\n"
        + "\n"
        + "QUERY MEASUREMENT FROM CACHE_LOINC1 {\n"
        + "    ASSERT(EQUALS(*result_datatype*, \"numeric\"))\n"
        + "    PID = *ptid*\n"
        + "    LOINC= *loinc*\n"
        + "    UNIT = *result_unit*\n"
        + "    VALUE_AS_NUMBER = *test_result*\n"
        + "    VALUE_AS_STRING = NULL\n"
        + "    MEASUREMENT_DATE = *primary_date*\n"
        + "\n"
        + "}\n"
        + "\n"
        + "QUERY MEASUREMENT FROM CACHE_LOINC1 {\n"
        + "    ASSERT(EQUALS(*result_datatype*, \"text\"))\n"
        + "    PID = *ptid*\n"
        + "    LOINC = *loinc*\n"
        + "    UNIT = *result_unit*\n"
        + "    VALUE_AS_NUMBER = NULL\n"
        + "    VALUE_AS_STRING = *test_result*\n"
        + "    MEASUREMENT_DATE = *primary_date*\n"
        + "\n"
        + "}\n"
        + "\n"
        + "CONNECTION CACHE_LOINC2 FROM OPTUM {\n"
        + "CACHE = loincs2\n"
        + "SORT COLUMN = *ptid*\n"
        + "QUERY = SELECT\n"
        + "            ptid,\n"
        + "            test_result,\n"
        + "            loinc,\n"
        + "            COALESCE(collected_date, result_date) AS primary_date,\n"
        + "            result_unit,\n"
        + "            result_datatype\n"
        + "       FROM $DATA_SCHEMA.optum_ehr_202508__lab where result_datatype is not null and result_date > '2016-01-01' and result_date < '2019-01-01'\n"
        + "\n"
        + "}\n"
        + "\n"
        + "\n"
        + "QUERY MEASUREMENT FROM CACHE_LOINC2 {\n"
        + "    ASSERT(EQUALS(*result_datatype*, \"numeric\"))\n"
        + "    PID = *ptid*\n"
        + "    LOINC= *loinc*\n"
        + "    UNIT = *result_unit*\n"
        + "    VALUE_AS_NUMBER = *test_result*\n"
        + "    VALUE_AS_STRING = NULL\n"
        + "    MEASUREMENT_DATE = *primary_date*\n"
        + "\n"
        + "}\n"
        + "\n"
        + "QUERY MEASUREMENT FROM CACHE_LOINC2 {\n"
        + "    ASSERT(EQUALS(*result_datatype*, \"text\"))\n"
        + "    PID = *ptid*\n"
        + "    LOINC = *loinc*\n"
        + "    UNIT = *result_unit*\n"
        + "    VALUE_AS_NUMBER = NULL\n"
        + "    VALUE_AS_STRING = *test_result*\n"
        + "    MEASUREMENT_DATE = *primary_date*\n"
        + "\n"
        + "}\n"
        + "\n"
        + "CONNECTION CACHE_LOINC3 FROM OPTUM {\n"
        + "CACHE = loincs3\n"
        + "SORT COLUMN = *ptid*\n"
        + "QUERY = SELECT\n"
        + "            ptid,\n"
        + "            test_result,\n"
        + "            loinc,\n"
        + "            COALESCE(collected_date, result_date) AS primary_date,\n"
        + "            result_unit,\n"
        + "            result_datatype\n"
        + "       FROM $DATA_SCHEMA.optum_ehr_202508__lab where result_datatype is not null and result_date >= '2019-01-01'\n"
        + "\n"
        + "}\n"
        + "\n"
        + "\n"
        + "QUERY MEASUREMENT FROM CACHE_LOINC3 {\n"
        + "    ASSERT(EQUALS(*result_datatype*, \"numeric\"))\n"
        + "    PID = *ptid*\n"
        + "    LOINC= *loinc*\n"
        + "    UNIT = *result_unit*\n"
        + "    VALUE_AS_NUMBER = *test_result*\n"
        + "    VALUE_AS_STRING = NULL\n"
        + "    MEASUREMENT_DATE = *primary_date*\n"
        + "\n"
        + "}\n"
        + "\n"
        + "QUERY MEASUREMENT FROM CACHE_LOINC3 {\n"
        + "    ASSERT(EQUALS(*result_datatype*, \"text\"))\n"
        + "    PID = *ptid*\n"
        + "    LOINC = *loinc*\n"
        + "    UNIT = *result_unit*\n"
        + "    VALUE_AS_NUMBER = NULL\n"
        + "    VALUE_AS_STRING = *test_result*\n"
        + "    MEASUREMENT_DATE = *primary_date*\n"
        + "\n"
        + "}\n"
        + "\n"
        + "#---------------------------------OBSERVATION------------------------------------------------\n"
        + "\n"
        + "CONNECTION OBSERVATION_SBP FROM OPTUM {\n"
        + "   CACHE = observations_sbp\n"
        + "   SORT COLUMN = *ptid*\n"
        + "   QUERY = SELECT\n"
        + "               ptid,\n"
        + "               obs_type,\n"
        + "               obs_date,\n"
        + "               CAST(obs_result as DOUBLE) as result\n"
        + "            from $DATA_SCHEMA.optum_ehr_202508__obs where obs_type = \"SBP\"\n"
        + "}\n"
        + "\n"
        + "QUERY MEASUREMENT FROM OBSERVATION_SBP{\n"
        + "    PID = *ptid*\n"
        + "    LOINC = \"8480-6\"\n"
        + "    LOINC.NAME = \"SYSTOLIC BLOOD PRESSURE\"\n"
        + "    UNIT = \"mm[Hg]\"\n"
        + "    VALUE_AS_NUMBER = *result*\n"
        + "    VALUE_AS_STRING = NULL\n"
        + "    MEASUREMENT_DATE = *obs_date*\n"
        + "}\n"
        + "\n"
        + "CONNECTION OBSERVATION_DBP FROM OPTUM {\n"
        + "   CACHE = observations_dbp\n"
        + "   SORT COLUMN = *ptid*\n"
        + "   QUERY = SELECT\n"
        + "               ptid,\n"
        + "               obs_type,\n"
        + "               obs_date,\n"
        + "               CAST(obs_result as DOUBLE) as result\n"
        + "            from $DATA_SCHEMA.optum_ehr_202508__obs where obs_type = \"DBP\"\n"
        + "}\n"
        + "\n"
        + "QUERY MEASUREMENT FROM OBSERVATION_DBP{\n"
        + "    PID = *ptid*\n"
        + "    LOINC = \"8462-4\"\n"
        + "    LOINC.NAME = \"DIASTOLIC BLOOD PRESSURE\"\n"
        + "    UNIT = \"mm[Hg]\"\n"
        + "    VALUE_AS_NUMBER = *result*\n"
        + "    VALUE_AS_STRING = NULL\n"
        + "    MEASUREMENT_DATE = *obs_date*\n"
        + "}\n"
        + "\n"
        + "CONNECTION OBSERVATION_BMI FROM OPTUM {\n"
        + "   CACHE = observations_bmi\n"
        + "   SORT COLUMN = *ptid*\n"
        + "   QUERY = SELECT\n"
        + "               ptid,\n"
        + "               obs_type,\n"
        + "               obs_date,\n"
        + "               CAST(obs_result as DOUBLE) as result\n"
        + "            from $DATA_SCHEMA.optum_ehr_202508__obs where obs_type = \"BMI\"\n"
        + "}\n"
        + "\n"
        + "QUERY MEASUREMENT FROM OBSERVATION_BMI{\n"
        + "    PID = *ptid*\n"
        + "    LOINC = \"59574-4\"\n"
        + "    LOINC.NAME = \"BMI\"\n"
        + "    UNIT = \"%\"\n"
        + "    VALUE_AS_NUMBER = *result*\n"
        + "    VALUE_AS_STRING = NULL\n"
        + "    MEASUREMENT_DATE = *obs_date*\n"
        + "}\n"
        + "\n"
        + "CONNECTION OBSERVATION_PULSE FROM OPTUM {\n"
        + "   CACHE = observations_pulse\n"
        + "   SORT COLUMN = *ptid*\n"
        + "   QUERY = SELECT\n"
        + "               ptid,\n"
        + "               obs_type,\n"
        + "               obs_date,\n"
        + "               CAST(obs_result as DOUBLE) as result\n"
        + "            from $DATA_SCHEMA.optum_ehr_202508__obs where obs_type = \"PULSE\"\n"
        + "}\n"
        + "\n"
        + "QUERY MEASUREMENT FROM OBSERVATION_PULSE{\n"
        + "    PID = *ptid*\n"
        + "    LOINC = \"8867-4\"\n"
        + "    LOINC.NAME = \"PULSE\"\n"
        + "    UNIT = \"bpm\"\n"
        + "    VALUE_AS_NUMBER = *result*\n"
        + "    VALUE_AS_STRING = NULL\n"
        + "    MEASUREMENT_DATE = *obs_date*\n"
        + "}\n"
        + "\n"
        + "CONNECTION OBSERVATION_HR FROM OPTUM {\n"
        + "   CACHE = observations_hr\n"
        + "   SORT COLUMN = *ptid*\n"
        + "   QUERY = SELECT\n"
        + "               ptid,\n"
        + "               obs_type,\n"
        + "               obs_date,\n"
        + "               CAST(obs_result as DOUBLE) as result\n"
        + "            from $DATA_SCHEMA.optum_ehr_202508__obs where obs_type = \"HR\"\n"
        + "}\n"
        + "\n"
        + "QUERY MEASUREMENT FROM OBSERVATION_HR{\n"
        + "    PID = *ptid*\n"
        + "    LOINC = \"8867-4\"\n"
        + "    LOINC.NAME = \"PULSE\"\n"
        + "    UNIT = \"bpm\"\n"
        + "    VALUE_AS_NUMBER = *result*\n"
        + "    VALUE_AS_STRING = NULL\n"
        + "    MEASUREMENT_DATE = *obs_date*\n"
        + "}\n"
        + "\n"
        + "CONNECTION OBSERVATION_TEMP FROM OPTUM {\n"
        + "   CACHE = observations_temp\n"
        + "   SORT COLUMN = *ptid*\n"
        + "   QUERY = SELECT\n"
        + "               ptid,\n"
        + "               obs_type,\n"
        + "               obs_date,\n"
        + "               CAST(obs_result as DOUBLE) as result\n"
        + "            from $DATA_SCHEMA.optum_ehr_202508__obs where obs_type = \"TEMP\"\n"
        + "}\n"
        + "\n"
        + "QUERY MEASUREMENT FROM OBSERVATION_TEMP{\n"
        + "    PID = *ptid*\n"
        + "    LOINC = \"8310-5\"\n"
        + "    LOINC.NAME = \"TEMP\"\n"
        + "    UNIT = \"deg c\"\n"
        + "    VALUE_AS_NUMBER = *result*\n"
        + "    VALUE_AS_STRING = NULL\n"
        + "    MEASUREMENT_DATE = *obs_date*\n"
        + "}\n"
        + "\n"
        + "CONNECTION OBSERVATION_WT FROM OPTUM {\n"
        + "   CACHE = observations_wt\n"
        + "   SORT COLUMN = *ptid*\n"
        + "   QUERY = SELECT\n"
        + "               ptid,\n"
        + "               obs_type,\n"
        + "               obs_date,\n"
        + "               CAST(obs_result as DOUBLE) as result\n"
        + "            from $DATA_SCHEMA.optum_ehr_202508__obs where obs_type = \"WT\"\n"
        + "}\n"
        + "\n"
        + "QUERY MEASUREMENT FROM OBSERVATION_WT{\n"
        + "    PID = *ptid*\n"
        + "    LOINC = \"29463-7\"\n"
        + "    LOINC.NAME = \"Weight\"\n"
        + "    UNIT = \"kg\"\n"
        + "    VALUE_AS_NUMBER = *result*\n"
        + "    VALUE_AS_STRING = NULL\n"
        + "    MEASUREMENT_DATE = *obs_date*\n"
        + "}\n"
        + "\n"
        + "CONNECTION OBSERVATION_HT FROM OPTUM {\n"
        + "   CACHE = observations_ht\n"
        + "   SORT COLUMN = *ptid*\n"
        + "   QUERY = SELECT\n"
        + "               ptid,\n"
        + "               obs_type,\n"
        + "               obs_date,\n"
        + "               CAST(obs_result as DOUBLE) as result\n"
        + "            from $DATA_SCHEMA.optum_ehr_202508__obs where obs_type = \"HT\"\n"
        + "}\n"
        + "\n"
        + "\n"
        + "QUERY MEASUREMENT FROM OBSERVATION_HT{\n"
        + "    PID = *ptid*\n"
        + "    LOINC = \"8302-2\"\n"
        + "    LOINC.NAME = \"Height\"\n"
        + "    UNIT = \"cm\"\n"
        + "    VALUE_AS_NUMBER = *result*\n"
        + "    VALUE_AS_STRING = NULL\n"
        + "    MEASUREMENT_DATE = *obs_date*\n"
        + "}\n"
        + "\n"
        + "CONNECTION OBSERVATION_LVEF FROM OPTUM {\n"
        + "   CACHE = observations_lvef\n"
        + "   SORT COLUMN = *ptid*\n"
        + "   QUERY = SELECT\n"
        + "               ptid,\n"
        + "               obs_type,\n"
        + "               obs_date,\n"
        + "               CAST(obs_result as DOUBLE) as result\n"
        + "            from $DATA_SCHEMA.optum_ehr_202508__obs where obs_type = \"LVEF\"\n"
        + "}\n"
        + "\n"
        + "QUERY MEASUREMENT FROM OBSERVATION_LVEF{\n"
        + "    PID = *ptid*\n"
        + "    LOINC = \"10230-1\"\n"
        + "    LOINC.NAME = \"LVEF\"\n"
        + "    UNIT = \"%\"\n"
        + "    VALUE_AS_NUMBER = *result*\n"
        + "    VALUE_AS_STRING = NULL\n"
        + "    MEASUREMENT_DATE = *obs_date*\n"
        + "}\n"
        + "\n"
        + "\n"
        + "CONNECTION LVEF FROM OPTUM {\n"
        + "   CACHE = lvef\n"
        + "   SORT COLUMN = *ptid*\n"
        + "   QUERY = SELECT ptid, note_date, res\n"
        + "                from (\n"
        + "                        select ptid,\n"
        + "                        note_date,\n"
        + "                        CAST(replace(concept_value, '%', '') as int) as res\n"
        + "                    from $DATA_SCHEMA.optum_ehr_202508__nlp_targeted where concept_name = 'LVEF'\n"
        + "                     ) subquery\n"
        + "                where res is not null\n"
        + "}\n"
        + "\n"
        + "\n"
        + "QUERY MEASUREMENT FROM LVEF {\n"
        + "    PID = *ptid*\n"
        + "    LOINC = \"10230-1\"\n"
        + "    LOINC.NAME = \"LVEF\"\n"
        + "    UNIT = \"%\"\n"
        + "    VALUE_AS_NUMBER = *res*\n"
        + "    VALUE_AS_STRING = NULL\n"
        + "    MEASUREMENT_DATE = *note_date*\n"
        + "}\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "#-------------------------------------RX-----------------------------------------\n"
        + "\n"
        + "\n"
        + "\n"
        + "SCHEMA RX {\n"
        + "  RX\n"
        + "  NDC\n"
        + "  ROUTE\n"
        + "  DRUG_EXPOSURE_DATE\n"
        + "  DRUG_EXPOSURE_TYPE\n"
        + "  PROVIDER_ID\n"
        + "}\n"
        + "\n"
        + "\n"
        + "\n"
        + "\n"
        + "CONNECTION CACHE_RX_ADM FROM OPTUM {\n"
        + "    SORT COLUMN= *ptid*\n"
        + "    CACHE = rx_adm\n"
        + "    QUERY =  SELECT\n"
        + "                m.ptid,\n"
        + "                m.ndc,\n"
        + "                coalesce(m.admin_date, m.order_date) as fill_dt,\n"
        + "                m.provid,\n"
        + "                m.route,\n"
        + "                rx.rxnorm as rxnorm\n"
        + "            from $DATA_SCHEMA.optum_ehr_202508__rx_adm m\n"
        + "                left join $DATA_SCHEMA.ndc_to_rx_v5 rx on m.ndc = rx.ndc\n"
        + "}\n"
        + "\n"
        + "\n"
        + "QUERY RX FROM CACHE_RX_ADM {\n"
        + "    PID = *ptid*\n"
        + "    RX = *rxnorm*\n"
        + "    NDC = *ndc*\n"
        + "    ROUTE = *route*\n"
        + "    DRUG_EXPOSURE_DATE.START = *fill_dt*\n"
        + "    DRUG_EXPOSURE_DATE.END = *fill_dt*\n"
        + "    DRUG_EXPOSURE_TYPE.CODE = \"RX ADM\"\n"
        + "    DRUG_EXPOSURE_TYPE.NAME = \"RX ADM\"\n"
        + "    PROVIDER_ID = *provid*\n"
        + "}\n"
        + "\n"
        + "CONNECTION CACHE_RX_IMMUN FROM OPTUM {\n"
        + "    SORT COLUMN= *ptid*\n"
        + "    CACHE = rx_immun\n"
        + "    QUERY =  SELECT\n"
        + "                m.ptid,\n"
        + "                m.ndc,\n"
        + "                m.immunization_date as fill_dt,\n"
        + "                rx.rxnorm as rxnorm\n"
        + "            from $DATA_SCHEMA.optum_ehr_202508__rx_immun m\n"
        + "                left join $DATA_SCHEMA.ndc_to_rx_v5 rx on m.ndc = rx.ndc\n"
        + "}\n"
        + "\n"
        + "\n"
        + "QUERY RX FROM CACHE_RX_IMMUN {\n"
        + "    PID = *ptid*\n"
        + "    RX = *rxnorm*\n"
        + "    NDC = *ndc*\n"
        + "    ROUTE = NULL\n"
        + "    DRUG_EXPOSURE_DATE.START = *fill_dt*\n"
        + "    DRUG_EXPOSURE_DATE.END = *fill_dt*\n"
        + "    DRUG_EXPOSURE_TYPE.CODE = \"RX IMMUN\"\n"
        + "    DRUG_EXPOSURE_TYPE.NAME = \"RX IMMUN\"\n"
        + "    PROVIDER_ID = NULL\n"
        + "}\n"
        + "\n"
        + "CONNECTION CACHE_RX_PATREP FROM OPTUM {\n"
        + "    SORT COLUMN= *ptid*\n"
        + "    CACHE = rx_patrep\n"
        + "    QUERY =  SELECT\n"
        + "                m.ptid,\n"
        + "                m.ndc,\n"
        + "                m.reported_date as fill_dt,\n"
        + "                m.route,\n"
        + "                rx.rxnorm as rxnorm\n"
        + "            from $DATA_SCHEMA.optum_ehr_202508__rx_patrep m\n"
        + "                left join $DATA_SCHEMA.ndc_to_rx_v5 rx on m.ndc = rx.ndc\n"
        + "}\n"
        + "\n"
        + "\n"
        + "QUERY RX FROM CACHE_RX_PATREP {\n"
        + "    PID = *ptid*\n"
        + "    RX = *rxnorm*\n"
        + "    NDC = *ndc*\n"
        + "    ROUTE = *route*\n"
        + "    DRUG_EXPOSURE_DATE.START = *fill_dt*\n"
        + "    DRUG_EXPOSURE_DATE.END = *fill_dt*\n"
        + "    DRUG_EXPOSURE_TYPE.CODE = \"RX PATREP\"\n"
        + "    DRUG_EXPOSURE_TYPE.NAME = \"RX PATREP\"\n"
        + "    PROVIDER_ID = NULL\n"
        + "}\n"
        + "\n"
        + "CONNECTION CACHE_RX_PRESCR FROM OPTUM {\n"
        + "    SORT COLUMN= *ptid*\n"
        + "    CACHE = rx_prescr\n"
        + "    QUERY =  SELECT\n"
        + "                m.ptid,\n"
        + "                m.rxdate,\n"
        + "                m.provid,\n"
        + "                m.route,\n"
        + "                m.ndc,\n"
        + "                rx.rxnorm as rxnorm,\n"
        + "                DATE(dateadd(day, m.days_supply, m.rxdate)) as end_date\n"
        + "            from $DATA_SCHEMA.optum_ehr_202508__rx_presc m\n"
        + "                left join $DATA_SCHEMA.ndc_to_rx_v5 rx on m.ndc = rx.ndc where days_supply < 3000\n"
        + "}\n"
        + "\n"
        + "QUERY RX FROM CACHE_RX_PRESCR {\n"
        + "    PID = *ptid*\n"
        + "    RX = *rxnorm*\n"
        + "    NDC = *ndc*\n"
        + "    ROUTE = *route*\n"
        + "    DRUG_EXPOSURE_DATE.START = *rxdate*\n"
        + "    DRUG_EXPOSURE_DATE.END = *end_date*\n"
        + "    DRUG_EXPOSURE_TYPE.CODE = \"RX PRESCR\"\n"
        + "    DRUG_EXPOSURE_TYPE.NAME = \"RX PRESCR\"\n"
        + "    PROVIDER_ID = *provid*\n"
        + "}\n"
        + "\n"
        + "\n"
        + "#--------------------------------VISIT---------------------------------\n"
        + "\n"
        + "CONNECTION VISIT FROM OPTUM {\n"
        + "    SORT COLUMN = *ptid*\n"
        + "    CACHE = visit\n"
        + "    QUERY = SELECT\n"
        + "                ptid,\n"
        + "                visit_start_date,\n"
        + "                visit_end_date,\n"
        + "                visit_type\n"
        + "            FROM $DATA_SCHEMA.optum_ehr_202508__vis\n"
        + "}\n"
        + "\n"
        + "SCHEMA VISITS {\n"
        + "    VISIT_TYPE\n"
        + "    VISIT_DATE\n"
        + "    PROVIDER_ID\n"
        + "}\n"
        + "\n"
        + "QUERY VISITS FROM VISIT {\n"
        + "    PID = *ptid*\n"
        + "    VISIT_TYPE = *visit_type*\n"
        + "    VISIT_DATE.START = *visit_start_date*\n"
        + "    VISIT_DATE.END = *visit_end_date*\n"
        + "    PROVIDER_ID = NULL\n"
        + "}\n"
        + "\n"
        + "CONNECTION ENCOUNTER FROM OPTUM {\n"
        + "    SORT COLUMN = *ptid*\n"
        + "    CACHE = encounter\n"
        + "    QUERY = SELECT\n"
        + "                d.ptid,\n"
        + "                d.interaction_date,\n"
        + "                d.interaction_type,\n"
        + "                e.provid\n"
        + "            from $DATA_SCHEMA.optum_ehr_202508__enc d\n"
        + "                LEFT JOIN (\n"
        + "                    SELECT\n"
        + "                        encid,\n"
        + "                        provid,\n"
        + "                        ROW_NUMBER() OVER (\n"
        + "                            PARTITION BY encid\n"
        + "                            ORDER BY\n"
        + "                                CASE\n"
        + "                                    WHEN provider_role = 'ATTENDING' THEN 1\n"
        + "                                    WHEN provider_role = 'ADMITTING' THEN 2\n"
        + "                                    ELSE 3\n"
        + "                                END,\n"
        + "                                provid\n"
        + "                        ) as rn\n"
        + "                    FROM\n"
        + "                        $DATA_SCHEMA.optum_ehr_202508__enc_prov\n"
        + "                ) e\n"
        + "                ON\n"
        + "                    d.encid = e.encid AND e.rn = 1\n"
        + "}\n"
        + "\n"
        + "QUERY VISITS FROM ENCOUNTER {\n"
        + "    PID = *ptid*\n"
        + "    VISIT_TYPE = *interaction_type*\n"
        + "    VISIT_DATE.START = *interaction_date*\n"
        + "    VISIT_DATE.END = *interaction_date*\n"
        + "    PROVIDER_ID = *provid*\n"
        + "}\n"
        + "\n"
        + "\n"
        + "#-----------------------------PROVIDER----------------------------\n"
        + "\n"
        + "CONNECTION CACHE_PROVIDER FROM OPTUM {\n"
        + "        SORT COLUMN = *provid*\n"
        + "        CACHE = provider\n"
        + "        QUERY = WITH ranked_providers AS (\n"
        + "                  SELECT\n"
        + "                    p.provid,\n"
        + "                    p.prim_spec_ind,\n"
        + "                    COALESCE(t.Specialization, p.specialty, t.Grouping) AS specialty,\n"
        + "                    ROW_NUMBER() OVER (PARTITION BY p.provid ORDER BY p.prim_spec_ind DESC) AS rn\n"
        + "                  FROM $DATA_SCHEMA.optum_ehr_202508__prov p\n"
        + "                      LEFT JOIN $DATA_SCHEMA.prov_tax t\n"
        + "                        ON p.taxonomy = t.Code\n"
        + "                    )\n"
        + "                    SELECT\n"
        + "                      provid,\n"
        + "                      specialty\n"
        + "                    FROM ranked_providers\n"
        + "                    WHERE rn = 1\n"
        + "\n"
        + "}\n"
        + "\n"
        + "\n"
        + "DATASET LEVEL SCHEMA PROVIDER {\n"
        + "PROVIDER_ID\n"
        + "NPI\n"
        + "CARE_SITE\n"
        + "PROVIDER_SPECIALTY\n"
        + "PROVIDER_TYPE\n"
        + "}\n"
        + "\n"
        + "QUERY PROVIDER FROM CACHE_PROVIDER {\n"
        + "  PROVIDER_ID = *provid*\n"
        + "  NPI = NULL\n"
        + "  CARE_SITE = NULL\n"
        + "  PROVIDER_SPECIALTY.CODE = *specialty*\n"
        + "  PROVIDER_SPECIALTY.NAME = *specialty*\n"
        + "  PROVIDER_TYPE = NULL\n"
        + "}\n"
        + "\n"
        + "\n"
        + "\n"
        + "#--------------------------------HIERS---------------------------------------\n"
        + "\n"
        + "\n"
        + "CONNECTION ICD10_HIER FROM OPTUM {\n"
        + "    CACHE=icd10.hier\n"
        + "    QUERY = SELECT CHILD_CODE, PARENT_CODE FROM $DATA_SCHEMA.derived_umls_icd10cm_hier\n"
        + "}\n"
        + "\n"
        + "CONNECTION ICD9_HIER FROM OPTUM {\n"
        + "    CACHE=icd9.hier\n"
        + "    QUERY = SELECT CHILD_CODE, PARENT_CODE FROM $DATA_SCHEMA.derived_umls_icd9_hier\n"
        + "}\n"
        + "\n"
        + "\n"
        + "CONNECTION CPT_DICT FROM OPTUM{\n"
        + "        CACHE = cpt.dict\n"
        + "        QUERY = select concept_code, concept_name FROM  $DATA_SCHEMA.vocab5_cpt_hcpcs\n"
        + "}\n"
        + "\n"
        + "TRANSFORM FROM CPT_DICT {\n"
        + "        SOURCE.FEATURE = \"CPT\"\n"
        + "        SOURCE.CODE = *concept_code*\n"
        + "        TARGET.NAME = *concept_name*\n"
        + "}\n"
        + "\n"
        + "CONNECTION ICD9_DICT FROM OPTUM {\n"
        + "        CACHE=icd9.dict\n"
        + "        QUERY=select concept_code, concept_name, code_np FROM  $DATA_SCHEMA.vocab5_icd9\n"
        + "}\n"
        + "\n"
        + "TRANSFORM FROM ICD9_DICT {\n"
        + "        SOURCE.FEATURE = \"ICD9\"\n"
        + "        SOURCE.CODE = *concept_code*\n"
        + "        TARGET.NAME = *concept_name*\n"
        + "\n"
        + "}\n"
        + "\n"
        + "MAP ICD10CODE FROM ICD10_DICT {\n"
        + "    KEY = *code_np*\n"
        + "    VALUE = *concept_code*\n"
        + "    CASE = FALSE\n"
        + "}\n"
        + "\n"
        + "MAP ICD9CODE FROM ICD9_DICT {\n"
        + "    KEY = *code_np*\n"
        + "    VALUE = *concept_code*\n"
        + "    CASE = FALSE\n"
        + "}\n"
        + "\n"
        + "CONNECTION ICD10_DICT FROM OPTUM{\n"
        + "        CACHE = icd10.dict\n"
        + "        QUERY = select concept_code, concept_name, code_np FROM  $DATA_SCHEMA.vocab5_icd10\n"
        + "}\n"
        + "\n"
        + "TRANSFORM FROM ICD10_DICT {\n"
        + "        SOURCE.FEATURE = \"ICD10\"\n"
        + "        SOURCE.CODE = *concept_code*\n"
        + "        TARGET.NAME = *concept_name*\n"
        + "\n"
        + "}\n"
        + "\n"
        + "\n"
        + "CONNECTION ATC_TO_RXNORM FROM OPTUM {\n"
        + "        CACHE = atc_to_rxnorm\n"
        + "        QUERY = select anc_concept_code, anc_concept_name, des_concept_code FROM  $VOCAB_SCHEMA.derived_atc_to_rxcui_map\n"
        + "}\n"
        + "\n"
        + "CONNECTION ATC_NAME FROM OPTUM {\n"
        + "        CACHE = atc_name\n"
        + "        QUERY = SELECT distinct anc_concept_code, anc_concept_name FROM $VOCAB_SCHEMA.derived_atc_to_rxcui_map\n"
        + "}\n"
        + "\n"
        + "HIERARCHY FROM ATC_TO_RXNORM {\n"
        + "        CHILD.FEATURE = \"RX\"\n"
        + "        PARENT.FEATURE = \"ATC\"\n"
        + "        CHILD.CODE = *des_concept_code*\n"
        + "        PARENT.CODE = *anc_concept_code*\n"
        + "}\n"
        + "\n"
        + "TRANSFORM FROM ATC_NAME {\n"
        + "        SOURCE.FEATURE = \"ATC\"\n"
        + "        SOURCE.CODE = *anc_concept_code*\n"
        + "        TARGET.NAME = *anc_concept_name*\n"
        + "}\n"
        + "\n"
        + "CONNECTION ATC_HIER FROM OPTUM{\n"
        + "    CACHE = atc_atc.hier\n"
        + "    QUERY = SELECT des_concept_code, anc_concept_code from $VOCAB_SCHEMA.derived_atc_to_atc_map\n"
        + "}\n"
        + "\n"
        + "HIERARCHY FROM ATC_HIER {\n"
        + "    CHILD.FEATURE = \"ATC\"\n"
        + "    PARENT.FEATURE = \"ATC\"\n"
        + "    CHILD.CODE = *des_concept_code*\n"
        + "    PARENT.CODE = *anc_concept_code*\n"
        + "}\n"
        + "\n"
        + "CONNECTION RXNORM_TO_RXNORM FROM OPTUM {\n"
        + "        CACHE=rxnorm_to_rxnorm.hier\n"
        + "        QUERY = select anc_concept_code, anc_concept_name, des_concept_code, des_concept_name FROM $VOCAB_SCHEMA.derived_rxcui_to_rxcui_map\n"
        + "}\n"
        + "\n"
        + "HIERARCHY FROM RXNORM_TO_RXNORM {\n"
        + "        CHILD.FEATURE = \"RX\"\n"
        + "        PARENT.FEATURE = \"RX\"\n"
        + "        CHILD.CODE = *des_concept_code*\n"
        + "        PARENT.CODE = *anc_concept_code*\n"
        + "}\n"
        + "\n"
        + "CONNECTION RX_DICT FROM OPTUM {\n"
        + "        CACHE = rx.dict\n"
        + "        QUERY = SELECT distinct concept_code, concept_name from $VOCAB_SCHEMA.vocab5_rx\n"
        + "}\n"
        + "\n"
        + "TRANSFORM FROM RX_DICT {\n"
        + "        SOURCE.FEATURE = \"RX\"\n"
        + "        SOURCE.CODE = *concept_code*\n"
        + "        TARGET.NAME = *concept_name*\n"
        + "}\n"
        + "\n"
        + "\n"
        + "CONNECTION ICD10PCS_DICT FROM OPTUM {\n"
        + "        CACHE = icd10pcs.dict\n"
        + "        QUERY = select concept_code, concept_name FROM  $DATA_SCHEMA.vocabv5_icd10pcs\n"
        + "}\n"
        + "\n"
        + "TRANSFORM FROM ICD10PCS_DICT {\n"
        + "        SOURCE.FEATURE = \"ICD10PCS\"\n"
        + "        SOURCE.CODE = *concept_code*\n"
        + "        TARGET.NAME = *concept_name*\n"
        + "}\n"
        + "\n"
        + "CONNECTION ICD10PCS_HIER FROM OPTUM {\n"
        + "    CACHE=icd10pcs.hier\n"
        + "    QUERY = SELECT CHILD_CODE, PARENT_CODE FROM $VOCAB_SCHEMA.derived_umls_icd10pcs_hier\n"
        + "}\n"
        + "\n"
        + "HIERARCHY FROM ICD10PCS_HIER {\n"
        + "    CHILD.FEATURE = \"ICD10PCS\"\n"
        + "    PARENT.FEATURE = \"ICD10PCS\"\n"
        + "    CHILD.CODE = *CHILD_CODE*\n"
        + "    PARENT.CODE = *PARENT_CODE*\n"
        + "}\n"
        + "\n"
        + "HIERARCHY FROM ICD9_HIER {\n"
        + "    CHILD.FEATURE = \"ICD9\"\n"
        + "    PARENT.FEATURE = \"ICD9\"\n"
        + "    CHILD.CODE = *CHILD_CODE*\n"
        + "}\n"
        + "\n"
        + "HIERARCHY FROM ICD10_HIER {\n"
        + "    CHILD.FEATURE = \"ICD10\"\n"
        + "    PARENT.FEATURE = \"ICD10\"\n"
        + "    CHILD.CODE = *CHILD_CODE*\n"
        + "}\n"
        + "\n"
        + "\n"
        + "CONNECTION LOINC_DICT FROM OPTUM {\n"
        + "    CACHE= loinc_names.dict\n"
        + "    QUERY = select concept_code, concept_name FROM $DATA_SCHEMA.vocab5_loinc\n"
        + "}\n"
        + "\n"
        + "TRANSFORM FROM LOINC_DICT {\n"
        + "        SOURCE.FEATURE = \"LOINC\"\n"
        + "        SOURCE.CODE = *concept_code*\n"
        + "        TARGET.NAME = *concept_name*\n"
        + "}\n"
        + "\n"
        + "CONNECTION VISIT_TYPE_HIER {\n"
        + "    FILE=visit.hier\n"
        + "}\n"
        + "\n"
        + "HIERARCHY FROM VISIT_TYPE_HIER {\n"
        + "    CHILD.FEATURE=\"VISIT_TYPE\"\n"
        + "    PARENT.FEATURE = \"VISIT_TYPE\"\n"
        + "    CHILD.CODE=C1\n"
        + "    PARENT.CODE=C2\n"
        + "}\n"
        + "\n"
        + "CONNECTION VISIT_DICT {\n"
        + "    FILE=visit.dict\n"
        + "}\n"
        + "\n"
        + "TRANSFORM FROM VISIT_DICT {\n"
        + "        SOURCE.FEATURE = \"VISIT_TYPE\"\n"
        + "        SOURCE.CODE = C1\n"
        + "        TARGET.CODE = C2\n"
        + "}\n"
        + "\n"
        + "CONNECTION GENDER_DICT {\n"
        + "    FILE=gender.dict\n"
        + "}\n"
        + "\n"
        + "TRANSFORM FROM GENDER_DICT {\n"
        + "    SOURCE.FEATURE = \"GENDER\"\n"
        + "    TARGET.FEATURE = \"GENDER\"\n"
        + "    SOURCE.CODE = C1\n"
        + "    TARGET.CODE = C2\n"
        + "    TARGET.NAME = C2\n"
        + "}\n"
        + "\n"
        + "CONNECTION ETHNICITY_DICT {\n"
        + "    FILE=ethnicity.dict\n"
        + "}\n"
        + "\n"
        + "TRANSFORM FROM ETHNICITY_DICT {\n"
        + "    SOURCE.FEATURE = \"ETHNICITY\"\n"
        + "    TARGET.FEATURE = \"ETHNICITY\"\n"
        + "    SOURCE.CODE = C1\n"
        + "    TARGET.CODE = C2\n"
        + "    TARGET.NAME = C2\n"
        + "}\n"
        + "\n"
        + "CONNECTION RACE_DICT {\n"
        + "    FILE=race.dict\n"
        + "}\n"
        + "\n"
        + "TRANSFORM FROM RACE_DICT {\n"
        + "    SOURCE.FEATURE = \"RACE\"\n"
        + "    TARGET.FEATURE = \"RACE\"\n"
        + "    SOURCE.CODE = C1\n"
        + "    TARGET.CODE = C2\n"
        + "    TARGET.NAME = C2\n"
        + "}\n"
        + "\n"
        + "CONNECTION NDC_DICT FROM OPTUM {\n"
        + "        CACHE = ndc.dict\n"
        + "        QUERY = SELECT distinct concept_code, concept_name from $VOCAB_SCHEMA.vocab5_ndc\n"
        + "}\n"
        + "\n"
        + "TRANSFORM FROM NDC_DICT {\n"
        + "        SOURCE.FEATURE = \"NDC\"\n"
        + "        SOURCE.CODE = C1\n"
        + "        TARGET.NAME = C2\n"
        + "}\n"
        + "\n"
        + "\n"
        + "\n"
        + "TQL UTILIZATION {\n"
        + "VAR DIAGNOSES = UNION(EXTEND BY(ICD9, START, START + 1 DAY), EXTEND BY(ICD10, START, START + 1 DAY))\n"
        + "VAR PROCEDURES = UNION(EXTEND BY(ICD10PCS, START, START + 1 DAY), EXTEND BY(CPT, START, START + 1 DAY))\n"
        + "VAR MEDICATIONS = EXTEND BY(RX, START, START + 1 DAY)\n"
        + "UNION($DIAGNOSES, $PROCEDURES, $MEDICATIONS)\n"
        + "}\n"
        + "\n"
        + "CSV RX.RX {\n"
        + "    C3 = ROUTE=$ROUTE.CODE\n"
        + "    C4 = DRUG_EXPOSURE_TYPE=$DRUG_EXPOSURE_TYPE.CODE\n"
        + "}\n"
        + "\n"
        + "CSV RX.NDC {\n"
        + "    C3 = ROUTE=$ROUTE.CODE\n"
        + "    C4 = DRUG_EXPOSURE_TYPE=$DRUG_EXPOSURE_TYPE.CODE\n"
        + "}\n"
        + "\n"
        + "\n"
        + "CSV MEASUREMENT.LOINC {\n"
        + "    C3 = $VALUE_AS_STRING.CODE [$UNIT]\n"
        + "    C4 = $VALUE_AS_NUMBER\n"
        + "}\n"
        + "\n"
        + "CSV ICD9.ICD9 {\n"
        + "    C3 = PRIMARY=FALSE;ORIGINAL=$ORIGINAL\n"
        + "}\n"
        + "CSV ICD10.ICD10 {\n"
        + "    C3 = PRIMARY=FALSE;ORIGINAL=$ORIGINAL\n"
        + "}\n"
        + "\n"
        + "\n"
        + "\n"
        + "#------- DATASET DATE\n"
        + "TQL DATASET_DATE {\n"
        + "DATE(\"1890-01-01\", \"2025-06-30\")\n"
        + "}"; // Will be populated from file input
  }

  public void parse(String pslContent) {
    // Remove comments
    pslContent = removeComments(pslContent);

    // Parse dataset info first
    parseDataset(pslContent);

    // Parse variables first (VARIABLE definitions)
    parseVariables(pslContent);

    // Parse feature definitions
    parseFeatures(pslContent);

    // Parse connections
    parseConnections(pslContent);

    // Parse schema definitions (separate from queries)
    parseSchemas(pslContent);

    // Parse query blocks (separate from schemas)
    parseQueries(pslContent);

    // Parse transforms (vocabulary mappings)
    parseTransforms(pslContent);

    // Parse hierarchies
    parseHierarchies(pslContent);
  }


  private void parseTransforms(String content) {
    Pattern transformPattern = Pattern.compile(
        "TRANSFORM\\s+FROM\\s+(\\w+)\\s*\\{([^}]+)\\}",
        Pattern.DOTALL
    );
    Matcher matcher = transformPattern.matcher(content);

    while (matcher.find()) {
      String connectionName = matcher.group(1);
      String transformBody = matcher.group(2);

      // Extract SOURCE.FEATURE
      Pattern featurePattern = Pattern.compile(
          "SOURCE\\.FEATURE\\s*=\\s*\"([^\"]+)\"",
          Pattern.CASE_INSENSITIVE
      );
      Matcher featureMatcher = featurePattern.matcher(transformBody);

      String sourceFeature = null;
      if (featureMatcher.find()) {
        sourceFeature = featureMatcher.group(1);
      }

      if (sourceFeature != null) {
        // Get the connection to find the source
        Connection conn = connections.get(connectionName);
        String vocabularySource = null;

        if (conn != null) {
          if (conn.query != null && !conn.query.isEmpty()) {
            // Extract table from query
            vocabularySource = extractTableFromQuery(conn.query);
          }
        } else {
          // Check if it's a file-based connection
          Pattern fileConnPattern = Pattern.compile(
              "CONNECTION\\s+" + connectionName + "\\s*\\{\\s*FILE\\s*=\\s*([^\\s}]+)",
              Pattern.DOTALL | Pattern.CASE_INSENSITIVE
          );
          Matcher fileConnMatcher = fileConnPattern.matcher(content);
          if (fileConnMatcher.find()) {
            vocabularySource = fileConnMatcher.group(1);
          }
        }

        if (vocabularySource != null) {
          transforms.add(new Transform(sourceFeature, vocabularySource));
        }
      }
    }
  }

  private String extractTableFromQuery(String query) {
    // Look for FROM clause with table name
    Pattern fromPattern = Pattern.compile(
        "FROM\\s+([\\w.]+)",
        Pattern.CASE_INSENSITIVE
    );
    Matcher matcher = fromPattern.matcher(query);

    if (matcher.find()) {
      return matcher.group(1);
    }

    return "UNKNOWN_TABLE";
  }


  private String removeComments(String content) {
    // Remove single-line comments starting with #
    return content.replaceAll("#[^\n]*", "");
  }

  private void parseVariables(String content) {
    Pattern varPattern = Pattern.compile("VARIABLE\\s+(\\w+)\\s*\\{([^}]*)\\}", Pattern.DOTALL);
    Matcher matcher = varPattern.matcher(content);

    while (matcher.find()) {
      String varName = matcher.group(1);
      String varValue = matcher.group(2).trim();
      variables.put(varName, varValue);
    }
  }

  private void parseFeatures(String content) {
    // Match FEATURE lines up until the first CONNECTION or # line with dashes
    Pattern featurePattern = Pattern.compile("FEATURE\\s+(\\w+)\\s*,\\s*([^,]+)\\s*,\\s*(\\w+)(?:\\s*,\\s*(.*))?");

    String[] lines = content.split("\n");
    for (String line : lines) {
      Matcher matcher = featurePattern.matcher(line.trim());
      if (matcher.find()) {
        String featureName = matcher.group(1);
        String description = matcher.group(2).trim();
        String dataType = matcher.group(3);
        String attributes = matcher.group(4) != null ? matcher.group(4).trim() : "";

        features.put(featureName, new Feature(featureName, description, dataType, attributes));
      }

      // Stop at connection definitions
      if (line.trim().startsWith("CONNECTION")) {
        break;
      }
    }
  }

  private void parseHierarchies(String content) {
    Pattern hierarchyPattern = Pattern.compile(
        "HIERARCHY\\s+FROM\\s+(\\w+)\\s*\\{([^}]+)\\}",
        Pattern.DOTALL
    );
    Matcher matcher = hierarchyPattern.matcher(content);

    while (matcher.find()) {
      String connectionName = matcher.group(1);
      String hierarchyBody = matcher.group(2);

      // Extract CHILD.FEATURE
      Pattern childFeaturePattern = Pattern.compile(
          "CHILD\\.FEATURE\\s*=\\s*\"([^\"]+)\"",
          Pattern.CASE_INSENSITIVE
      );
      Matcher childFeatureMatcher = childFeaturePattern.matcher(hierarchyBody);
      String childFeature = null;
      if (childFeatureMatcher.find()) {
        childFeature = childFeatureMatcher.group(1);
      }

      // Extract PARENT.FEATURE
      Pattern parentFeaturePattern = Pattern.compile(
          "PARENT\\.FEATURE\\s*=\\s*\"([^\"]+)\"",
          Pattern.CASE_INSENSITIVE
      );
      Matcher parentFeatureMatcher = parentFeaturePattern.matcher(hierarchyBody);
      String parentFeature = null;
      if (parentFeatureMatcher.find()) {
        parentFeature = parentFeatureMatcher.group(1);
      }

      // Extract CHILD.CODE
      Pattern childCodePattern = Pattern.compile(
          "CHILD\\.CODE\\s*=\\s*(.+?)(?=\\s*$|\\s*PARENT)",
          Pattern.CASE_INSENSITIVE | Pattern.MULTILINE
      );
      Matcher childCodeMatcher = childCodePattern.matcher(hierarchyBody);
      String childCode = null;
      if (childCodeMatcher.find()) {
        childCode = childCodeMatcher.group(1).trim();
      }

      // Extract PARENT.CODE
      Pattern parentCodePattern = Pattern.compile(
          "PARENT\\.CODE\\s*=\\s*(.+?)(?=\\s*$)",
          Pattern.CASE_INSENSITIVE | Pattern.MULTILINE
      );
      Matcher parentCodeMatcher = parentCodePattern.matcher(hierarchyBody);
      String parentCode = null;
      if (parentCodeMatcher.find()) {
        parentCode = parentCodeMatcher.group(1).trim();
      }

      if (childFeature != null && parentFeature != null && childCode != null && parentCode != null) {
        // Get the connection to find the source
        Connection conn = connections.get(connectionName);
        String sourceTable = null;

        if (conn != null && conn.query != null && !conn.query.isEmpty()) {
          // Extract table from query
          sourceTable = extractTableFromQuery(conn.query);
        } else {
          // Check if it's a file-based connection
          Pattern fileConnPattern = Pattern.compile(
              "CONNECTION\\s+" + connectionName + "\\s*\\{\\s*FILE\\s*=\\s*([^\\s}]+)",
              Pattern.DOTALL | Pattern.CASE_INSENSITIVE
          );
          Matcher fileConnMatcher = fileConnPattern.matcher(content);
          if (fileConnMatcher.find()) {
            sourceTable = fileConnMatcher.group(1);
          }
        }

        if (sourceTable != null) {
          // Process child and parent codes to extract column names
          String childColumn = extractColumnFromCode(childCode);
          String parentColumn = extractColumnFromCode(parentCode);

          hierarchies.add(new Hierarchy(
              childFeature,
              parentFeature,
              sourceTable,
              childColumn,
              parentColumn
          ));
        }
      }
    }
  }

  private String extractColumnFromCode(String code) {
    // Check if it's wrapped in *asterisks*
    Pattern columnPattern = Pattern.compile("\\*([^*]+)\\*");
    Matcher matcher = columnPattern.matcher(code);
    if (matcher.find()) {
      return matcher.group(1);
    }
    // Otherwise return as-is (e.g., C1, C2)
    return code;
  }

  private void parseConnections(String content) {
    Pattern connPattern = Pattern.compile(
        "CONNECTION\\s+(\\w+)\\s+FROM\\s+(\\w+)\\s*\\{([^}]+)\\}",
        Pattern.DOTALL
    );
    Matcher matcher = connPattern.matcher(content);

    while (matcher.find()) {
      String connName = matcher.group(1);
      String fromSource = matcher.group(2);
      String connBody = matcher.group(3);

      // Extract QUERY
      Pattern queryPattern = Pattern.compile("QUERY\\s*=\\s*(.+?)(?=\\s*\\}|$)", Pattern.DOTALL);
      Matcher queryMatcher = queryPattern.matcher(connBody);

      String query = "";
      if (queryMatcher.find()) {
        query = queryMatcher.group(1).trim();
        // Substitute variables
        query = substituteVariables(query);
      }

      connections.put(connName, new Connection(connName, fromSource, query));
    }
  }

  private void parseSchemas(String content) {
    // Parse SCHEMA definitions (with optional DEDUPLICATED and PATIENT LEVEL)
    Pattern schemaPattern = Pattern.compile(
        "(DEDUPLICATED\\s+)?(PATIENT\\s+LEVEL\\s+)?SCHEMA\\s+(\\w+)\\s*\\{([^}]+)\\}",
        Pattern.DOTALL
    );
    Matcher matcher = schemaPattern.matcher(content);

    while (matcher.find()) {
      boolean isDeduplicated = matcher.group(1) != null;
      boolean isPatientLevel = matcher.group(2) != null;
      String schemaName = matcher.group(3);
      String schemaBody = matcher.group(4);

      // Parse schema features
      List<String> schemaFeatures = parseSchemaFeatures(schemaBody);

      schemas.put(schemaName, new SchemaDefinition(schemaName, isDeduplicated, isPatientLevel, schemaFeatures));
    }
  }

  private void parseQueries(String content) {
    // Parse QUERY blocks independently of SCHEMA blocks
    Pattern queryPattern = Pattern.compile(
        "QUERY\\s+(\\w+)\\s+FROM\\s+(\\w+)\\s*\\{([^}]+)\\}",
        Pattern.DOTALL
    );
    Matcher matcher = queryPattern.matcher(content);

    while (matcher.find()) {
      String schemaName = matcher.group(1);
      String connectionName = matcher.group(2);
      String queryBody = matcher.group(3);

      // Skip if this is not a valid schema (e.g., it's a hierarchy query)
      SchemaDefinition schema = schemas.get(schemaName);
      if (schema == null) {
        continue; // Not a data schema query
      }

      // Parse query mappings
      Map<String, List<FeatureMapping>> featureMappings = parseQueryMappings(queryBody);

      Query query = new Query(
          schemaName,
          connectionName,
          schema.isDeduplicated,
          schema.isPatientLevel,
          schema.features,
          featureMappings
      );
      queries.add(query);
    }
  }

  private List<String> parseSchemaFeatures(String schemaBody) {
    List<String> features = new ArrayList<>();
    String[] lines = schemaBody.split("\n");
    for (String line : lines) {
      line = line.trim();
      if (!line.isEmpty() && !line.startsWith("//")) {
        features.add(line);
      }
    }
    return features;
  }

  private void parseDataset(String content) {
    Pattern datasetPattern = Pattern.compile(
        "DATASET\\s+(\\w+)\\s*\\{([^}]+)\\}",
        Pattern.DOTALL
    );
    Matcher matcher = datasetPattern.matcher(content);

    if (matcher.find()) {
      datasetName = matcher.group(1);
      String datasetBody = matcher.group(2);

      // Extract DATASET_VERSION
      Pattern versionPattern = Pattern.compile(
          "DATASET_VERSION\\s*=\\s*([^\\s\\n]+)",
          Pattern.CASE_INSENSITIVE
      );
      Matcher versionMatcher = versionPattern.matcher(datasetBody);
      if (versionMatcher.find()) {
        datasetVersion = versionMatcher.group(1);
      }
    }
  }

  private Map<String, List<FeatureMapping>> parseQueryMappings(String queryBody) {
    Map<String, List<FeatureMapping>> mappings = new HashMap<>();
    String[] lines = queryBody.split("\n");

    for (String line : lines) {
      line = line.trim();
      if (line.isEmpty() || line.startsWith("PID") || line.startsWith("ASSERT")) {
        continue;
      }

      // Match patterns like: ICD10 = *diagnosis_cd* or ICD10.START = *condition_start_date*
      Pattern mappingPattern = Pattern.compile("(\\w+)(?:\\.(\\w+))?\\s*=\\s*(.+)");
      Matcher matcher = mappingPattern.matcher(line);

      if (matcher.find()) {
        String feature = matcher.group(1);
        String suffix = matcher.group(2); // Could be START, END, NAME, CODE
        String value = matcher.group(3).trim();

        // Check for NULL
        if (value.equals("NULL")) {
          if (!mappings.containsKey(feature)) {
            mappings.put(feature, new ArrayList<>());
          }
          mappings.get(feature).add(new FeatureMapping(null, "NULL", suffix));
          continue;
        }

        // Check for literal string values in quotes
        Pattern literalPattern = Pattern.compile("\"([^\"]+)\"");
        Matcher literalMatcher = literalPattern.matcher(value);

        if (literalMatcher.find()) {
          String literalValue = literalMatcher.group(1);
          if (!mappings.containsKey(feature)) {
            mappings.put(feature, new ArrayList<>());
          }
          // Only add CODE suffix mappings to avoid duplicate entries for NAME/CODE
          if (!"NAME".equals(suffix)) {
            mappings.get(feature).add(new FeatureMapping(null, "\"" + literalValue + "\"", suffix));
          }
          continue;
        }

        // Extract column reference from *column* or MAP(..., *column*)
        Pattern columnPattern = Pattern.compile("\\*([^*]+)\\*");
        Matcher columnMatcher = columnPattern.matcher(value);

        if (columnMatcher.find()) {
          String column = columnMatcher.group(1);
          if (!mappings.containsKey(feature)) {
            mappings.put(feature, new ArrayList<>());
          }

          // Add the mapping, but track whether it's a NAME or CODE suffix
          // Only add CODE suffix mappings to avoid duplicate entries
          if (!"NAME".equals(suffix)) {
            mappings.get(feature).add(new FeatureMapping(column, value, suffix));
          }
        }
      }
    }

    return mappings;
  }

  private String substituteVariables(String text) {
    for (Map.Entry<String, String> entry : variables.entrySet()) {
      text = text.replace("$" + entry.getKey(), entry.getValue());
    }
    return text;
  }

  public void printLineage() {
    System.out.println("=".repeat(80));
    System.out.println(datasetName + " " + datasetVersion + " FEATURE LINEAGE MAP");
    System.out.println("=".repeat(80));
    System.out.println();

    // Group queries by schema name
    Map<String, List<Query>> schemaGroups = new LinkedHashMap<>();
    for (Query query : queries) {
      schemaGroups.computeIfAbsent(query.schemaName, k -> new ArrayList<>()).add(query);
    }

    // Print each schema group
    for (Map.Entry<String, List<Query>> entry : schemaGroups.entrySet()) {
      String schemaName = entry.getKey();
      List<Query> schemaQueries = entry.getValue();

      // Get schema type from first query (they should all be the same)
      Query firstQuery = schemaQueries.get(0);
      StringBuilder schemaType = new StringBuilder();
      if (firstQuery.isDeduplicated) schemaType.append("DEDUPLICATED ");
      if (firstQuery.isPatientLevel) schemaType.append("PATIENT LEVEL ");
      String schemaTypeStr = schemaType.length() > 0 ? " (" + schemaType.toString().trim() + ")" : "";

      System.out.println("SCHEMA: " + schemaName + schemaTypeStr);

      // Group queries by connection name to consolidate duplicates
      Map<String, List<Query>> connectionGroups = new LinkedHashMap<>();
      for (Query query : schemaQueries) {
        connectionGroups.computeIfAbsent(query.connectionName, k -> new ArrayList<>()).add(query);
      }

      // Print each connection group
      int connectionIndex = 0;
      for (Map.Entry<String, List<Query>> connEntry : connectionGroups.entrySet()) {
        String connectionName = connEntry.getKey();
        List<Query> connectionQueries = connEntry.getValue();

        connectionIndex++;
        boolean isLastConnection = (connectionIndex == connectionGroups.size());
        String connPrefix = isLastConnection ? "└─" : "├─";
        String featurePrefix = isLastConnection ? "   " : "│  ";

        System.out.println(connPrefix + " CONNECTION: " + connectionName);
        System.out.println(featurePrefix + " │");

        Connection conn = connections.get(connectionName);
        if (conn == null) continue;

        // Parse source tables from connection query
        Map<String, String> tableAliases = parseTableAliases(conn.query);

        // Merge all feature mappings from all queries using this connection
        Map<String, Set<FeatureMapping>> consolidatedMappings = new LinkedHashMap<>();
        List<String> schemaFeatures = connectionQueries.get(0).schemaFeatures;

        for (Query query : connectionQueries) {
          for (Map.Entry<String, List<FeatureMapping>> mappingEntry : query.featureMappings.entrySet()) {
            String feature = mappingEntry.getKey();
            if (!consolidatedMappings.containsKey(feature)) {
              consolidatedMappings.put(feature, new LinkedHashSet<>());
            }
            consolidatedMappings.get(feature).addAll(mappingEntry.getValue());
          }
        }

        // Get all features that are actually mapped
        Set<String> mappedFeatures = consolidatedMappings.keySet();

        int featureCount = 0;
        for (String feature : schemaFeatures) {
          if (!mappedFeatures.contains(feature)) {
            continue; // Skip features not mapped
          }

          featureCount++;
          boolean isLastFeature = (featureCount == mappedFeatures.size());
          String featureBranch = isLastFeature ? "└───" : "├───";
          String sourcePrefix = isLastFeature ? "    " : "│   ";

          Set<FeatureMapping> mappings = consolidatedMappings.get(feature);

          System.out.println(featurePrefix + " " + featureBranch + " FEATURE: " + feature);

          if (mappings != null && !mappings.isEmpty()) {
            List<FeatureMapping> mappingList = new ArrayList<>(mappings);
            for (int j = 0; j < mappingList.size(); j++) {
              FeatureMapping mapping = mappingList.get(j);
              boolean isLastMapping = (j == mappingList.size() - 1);
              String mappingBranch = isLastMapping ? "└─>" : "├─>";

              if (mapping.column == null) {
                // Check if it's a literal string value (starts and ends with quotes)
                if (mapping.rawValue.startsWith("\"") && mapping.rawValue.endsWith("\"")) {
                  String suffixStr = mapping.suffix != null ? " (" + mapping.suffix + ")" : "";
                  System.out.println(featurePrefix + " " + sourcePrefix + " " + mappingBranch + " SOURCE: " + mapping.rawValue + suffixStr);
                } else {
                  // It's NULL or some other special value
                  System.out.println(featurePrefix + " " + sourcePrefix + " " + mappingBranch + " SOURCE: " + mapping.rawValue);
                }
              } else {
                String sourceTable = resolveSourceTable(mapping.column, tableAliases, conn.query);
                String suffixStr = mapping.suffix != null ? " (" + mapping.suffix + ")" : "";
                System.out.println(featurePrefix + " " + sourcePrefix + " " + mappingBranch + " SOURCE: " + sourceTable + "." + mapping.column + suffixStr);
              }
            }
          }
        }

        if (!isLastConnection) {
          System.out.println(featurePrefix);
        }
      }

      System.out.println();
    }

    // ============== ADD VOCABULARY SECTION HERE ==============
    // Print vocabulary section
    if (!transforms.isEmpty()) {
      System.out.println("=".repeat(80));
      System.out.println("VOCABULARY MAPPINGS");
      System.out.println("=".repeat(80));
      System.out.println();

      for (int i = 0; i < transforms.size(); i++) {
        Transform transform = transforms.get(i);
        boolean isLast = (i == transforms.size() - 1);
        String prefix = isLast ? "└───" : "├───";

        System.out.println(prefix + " FEATURE: " + transform.feature);
        System.out.println((isLast ? "    " : "│   ") + " └─> VOCABULARY SOURCE: " + transform.vocabularySource);

        if (!isLast) {
          System.out.println("│");
        }
      }
      System.out.println();
    }
    if (!hierarchies.isEmpty()) {
      System.out.println("=".repeat(80));
      System.out.println("FEATURE HIERARCHIES");
      System.out.println("=".repeat(80));
      System.out.println();

      for (int i = 0; i < hierarchies.size(); i++) {
        Hierarchy hierarchy = hierarchies.get(i);
        boolean isLast = (i == hierarchies.size() - 1);
        String prefix = isLast ? "└───" : "├───";
        String indent = isLast ? "    " : "│   ";

        System.out.println(prefix + " HIERARCHY: " + hierarchy.parentFeature + " to " + hierarchy.childFeature);
        System.out.println(indent + " ├─> CHILD.CODE = " + hierarchy.sourceTable + "." + hierarchy.childColumn);
        System.out.println(indent + " └─> PARENT.CODE = " + hierarchy.sourceTable + "." + hierarchy.parentColumn);

        if (!isLast) {
          System.out.println("│");
        }
      }
      System.out.println();
    }

    // =========================================================
  }

  private Map<String, String> parseTableAliases(String query) {
    Map<String, String> aliases = new HashMap<>();

    // Match patterns like: FROM schema.table alias or FROM schema.table AS alias
    Pattern fromPattern = Pattern.compile(
        "FROM\\s+([\\w.]+)\\s+(?:AS\\s+)?(\\w+)",
        Pattern.CASE_INSENSITIVE
    );
    Matcher matcher = fromPattern.matcher(query);

    while (matcher.find()) {
      String table = matcher.group(1);
      String alias = matcher.group(2);
      if (!alias.equalsIgnoreCase("WHERE") && !alias.equalsIgnoreCase("LEFT") &&
          !alias.equalsIgnoreCase("INNER") && !alias.equalsIgnoreCase("RIGHT") &&
          !alias.equalsIgnoreCase("JOIN")) {
        aliases.put(alias, table);
      }
    }

    // Parse JOINs with balanced parentheses for subqueries
    int i = 0;
    while (i < query.length()) {
      Pattern joinStart = Pattern.compile(
          "(?:LEFT|RIGHT|INNER|OUTER)?\\s*JOIN\\s+",
          Pattern.CASE_INSENSITIVE
      );
      Matcher joinMatcher = joinStart.matcher(query.substring(i));

      if (!joinMatcher.find()) {
        break;
      }

      int joinPos = i + joinMatcher.end();

      // Skip whitespace
      while (joinPos < query.length() && Character.isWhitespace(query.charAt(joinPos))) {
        joinPos++;
      }

      if (joinPos < query.length() && query.charAt(joinPos) == '(') {
        // Extract balanced parentheses
        int parenCount = 1;
        int start = joinPos + 1;
        int end = start;

        while (end < query.length() && parenCount > 0) {
          if (query.charAt(end) == '(') parenCount++;
          if (query.charAt(end) == ')') parenCount--;
          end++;
        }

        String subquery = query.substring(start, end - 1);

        // Get alias after closing paren
        Pattern aliasPattern = Pattern.compile(
            "^\\s*(?:AS\\s+)?(\\w+)",
            Pattern.CASE_INSENSITIVE
        );
        Matcher aliasMatcher = aliasPattern.matcher(query.substring(end));

        if (aliasMatcher.find()) {
          String alias = aliasMatcher.group(1);
          String innerTable = extractTableFromSubquery(subquery);
          if (innerTable != null) {
            aliases.put(alias, innerTable);
          }
        }

        i = end;
      } else {
        i = joinPos + 1;
      }
    }

    return aliases;
  }

  private String extractTableFromSubquery(String subquery) {
    Pattern fromPattern = Pattern.compile(
        "FROM\\s+([\\w.]+)",
        Pattern.CASE_INSENSITIVE
    );
    Matcher matcher = fromPattern.matcher(subquery);

    if (matcher.find()) {
      return matcher.group(1);
    }

    return null;
  }

  private String findColumnAliasInSelect(String column, String query) {
    Pattern selectPattern = Pattern.compile(
        "SELECT\\s+(.+?)\\s+FROM",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );
    Matcher matcher = selectPattern.matcher(query);

    if (!matcher.find()) {
      return null;
    }

    String selectClause = matcher.group(1);

    Pattern columnPattern = Pattern.compile(
        "(\\w+)\\.(\\w+)(?:\\s+(?:as\\s+)?(\\w+))?",
        Pattern.CASE_INSENSITIVE
    );
    Matcher colMatcher = columnPattern.matcher(selectClause);

    while (colMatcher.find()) {
      String alias = colMatcher.group(1);
      String actualColumn = colMatcher.group(2);
      String asName = colMatcher.group(3);

      if (actualColumn.equalsIgnoreCase(column) ||
          (asName != null && asName.equalsIgnoreCase(column))) {
        return alias;
      }
    }

    return null;
  }

  private String resolveSourceTable(String column, Map<String, String> tableAliases, String fullQuery) {
    if (column.contains(".")) {
      String alias = column.substring(0, column.indexOf("."));
      String table = tableAliases.get(alias);
      return table != null ? table : "UNKNOWN_TABLE";
    }

    String aliasForColumn = findColumnAliasInSelect(column, fullQuery);
    if (aliasForColumn != null && tableAliases.containsKey(aliasForColumn)) {
      return tableAliases.get(aliasForColumn);
    }

    Pattern fromPattern = Pattern.compile(
        "FROM\\s+([\\w.]+)(?:\\s+(?:AS\\s+)?\\w+)?",
        Pattern.CASE_INSENSITIVE
    );
    Matcher matcher = fromPattern.matcher(fullQuery);
    if (matcher.find()) {
      return matcher.group(1);
    }

    return "UNKNOWN_TABLE";
  }




  // Inner classes
  static class Feature {
    String name;
    String description;
    String dataType;
    String attributes;

    Feature(String name, String description, String dataType, String attributes) {
      this.name = name;
      this.description = description;
      this.dataType = dataType;
      this.attributes = attributes;
    }
  }

  static class Hierarchy {
    String childFeature;
    String parentFeature;
    String sourceTable;
    String childColumn;
    String parentColumn;

    Hierarchy(String childFeature, String parentFeature, String sourceTable,
        String childColumn, String parentColumn) {
      this.childFeature = childFeature;
      this.parentFeature = parentFeature;
      this.sourceTable = sourceTable;
      this.childColumn = childColumn;
      this.parentColumn = parentColumn;
    }
  }




  static class Transform {
    String feature;
    String vocabularySource;

    Transform(String feature, String vocabularySource) {
      this.feature = feature;
      this.vocabularySource = vocabularySource;
    }
  }


  static class Connection {
    String name;
    String fromSource;
    String query;

    Connection(String name, String fromSource, String query) {
      this.name = name;
      this.fromSource = fromSource;
      this.query = query;
    }
  }

  static class SchemaDefinition {
    String name;
    boolean isDeduplicated;
    boolean isPatientLevel;
    List<String> features;

    SchemaDefinition(String name, boolean isDeduplicated, boolean isPatientLevel, List<String> features) {
      this.name = name;
      this.isDeduplicated = isDeduplicated;
      this.isPatientLevel = isPatientLevel;
      this.features = features;
    }
  }

  static class Query {
    String schemaName;
    String connectionName;
    boolean isDeduplicated;
    boolean isPatientLevel;
    List<String> schemaFeatures;
    Map<String, List<FeatureMapping>> featureMappings;

    Query(String schemaName, String connectionName, boolean isDeduplicated, boolean isPatientLevel,
        List<String> schemaFeatures, Map<String, List<FeatureMapping>> featureMappings) {
      this.schemaName = schemaName;
      this.connectionName = connectionName;
      this.isDeduplicated = isDeduplicated;
      this.isPatientLevel = isPatientLevel;
      this.schemaFeatures = schemaFeatures;
      this.featureMappings = featureMappings;
    }
  }



  static class FeatureMapping {
    String column;
    String rawValue;
    String suffix;

    FeatureMapping(String column, String rawValue, String suffix) {
      this.column = column;
      this.rawValue = rawValue;
      this.suffix = suffix;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      FeatureMapping that = (FeatureMapping) o;
      return Objects.equals(column, that.column) &&
          Objects.equals(rawValue, that.rawValue) &&
          Objects.equals(suffix, that.suffix);
    }

    @Override
    public int hashCode() {
      return Objects.hash(column, rawValue, suffix);
    }
  }

}