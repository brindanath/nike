package test.com.brindys.deTools.pslFeatureMapper;


import com.brindys.ETLTools.pslFeatureMapper.PSLFeatureMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class PSLFeatureMapperTest {

  private PSLFeatureMapper mapper;
  private String samplePSL;

  @BeforeEach
  public void setUp() {
    mapper = new PSLFeatureMapper();
    samplePSL = loadSampleConfig();
  }

  @Test
  public void testBasicParsing() {
    assertDoesNotThrow(() -> mapper.parse(samplePSL));
  }

  @Test
  public void testDatasetNameAndVersion() {
    mapper.parse(samplePSL);

    // Capture output
    String output = captureOutput();

    assertTrue(output.contains("PHR_NVS v1.2.0 FEATURE LINEAGE MAP"));
  }

  @Test
  public void testSchemasParsed() {
    mapper.parse(samplePSL);

    String output = captureOutput();

    // Check for expected schemas
    assertTrue(output.contains("SCHEMA: PERSON"));
    assertTrue(output.contains("SCHEMA: ICD10"));
    assertTrue(output.contains("SCHEMA: ICD9"));
    assertTrue(output.contains("SCHEMA: MEASUREMENT"));
    assertTrue(output.contains("SCHEMA: RX"));
    assertTrue(output.contains("SCHEMA: PROVIDER"));
  }

  @Test
  public void testPatientLevelSchema() {
    mapper.parse(samplePSL);

    String output = captureOutput();

    assertTrue(output.contains("SCHEMA: PERSON (PATIENT LEVEL)"));
  }

  @Test
  public void testDeduplicatedSchema() {
    mapper.parse(samplePSL);

    String output = captureOutput();

    assertTrue(output.contains("SCHEMA: ICD10 (DEDUPLICATED)"));
  }

  @Test
  public void testFeatureLineage() {
    mapper.parse(samplePSL);

    String output = captureOutput();

    // Check for feature mappings
    assertTrue(output.contains("FEATURE: ICD10"));
    assertTrue(output.contains("FEATURE: GENDER"));
    assertTrue(output.contains("FEATURE: LOINC"));
  }

  @Test
  public void testSourceTableResolution() {
    mapper.parse(samplePSL);

    String output = captureOutput();

    // Check that source tables are resolved correctly
    assertTrue(output.contains("optum_ehr_202508__pt_clinical"));
    assertTrue(output.contains("optum_ehr_202508__diag"));
    assertTrue(output.contains("optum_ehr_202508__lab"));
  }

  @Test
  public void testLiteralValueMapping() {
    mapper.parse(samplePSL);

    String output = captureOutput();

    // Check for literal string values in quotes
    assertTrue(output.contains("\"REGION\""));
    assertTrue(output.contains("\"RX PRESCR\""));
  }

  @Test
  public void testNullMapping() {
    mapper.parse(samplePSL);

    String output = captureOutput();

    // Check for NULL mappings
    assertTrue(output.contains("SOURCE: NULL"));
  }

  @Test
  public void testVocabularyMappings() {
    mapper.parse(samplePSL);

    String output = captureOutput();

    // Check vocabulary section exists
    assertTrue(output.contains("VOCABULARY MAPPINGS"));

    // Check for specific vocabulary mappings
    assertTrue(output.contains("FEATURE: ICD9"));
    assertTrue(output.contains("VOCABULARY SOURCE:"));
    assertTrue(output.contains("vocab5_icd9"));
  }

  @Test
  public void testFileBasedVocabulary() {
    mapper.parse(samplePSL);

    String output = captureOutput();

    // Check for file-based vocabulary sources
    assertTrue(output.contains("VOCABULARY SOURCE: gender.dict"));
    assertTrue(output.contains("VOCABULARY SOURCE: race.dict"));
  }

  @Test
  public void testHierarchies() {
    mapper.parse(samplePSL);

    String output = captureOutput();

    // Check hierarchy section exists
    assertTrue(output.contains("FEATURE HIERARCHIES"));

    // Check for specific hierarchies
    assertTrue(output.contains("HIERARCHY: RX to RX"));
    assertTrue(output.contains("HIERARCHY: ATC to RX"));
    assertTrue(output.contains("CHILD.CODE"));
    assertTrue(output.contains("PARENT.CODE"));
  }

  @Test
  public void testVariableSubstitution() {
    mapper.parse(samplePSL);

    String output = captureOutput();

    // Check that variables were substituted in queries
    assertTrue(output.contains("unifyplus_eu_atropos_poc.atropos_pre_transformed"));
  }

  @Test
  public void testMultipleConnectionsConsolidated() {
    mapper.parse(samplePSL);

    String output = captureOutput();

    // CACHE_LOINC1 should appear only once for MEASUREMENT schema
    // (even though there are multiple QUERY blocks using it)
    int count = countOccurrences(output, "CONNECTION: CACHE_LOINC1");
    assertEquals(1, count, "CACHE_LOINC1 should be consolidated into one connection");
  }

  @Test
  public void testDateSuffixMapping() {
    mapper.parse(samplePSL);

    String output = captureOutput();

    // Check for START and END suffixes on date fields
    assertTrue(output.contains("(START)"));
    assertTrue(output.contains("(END)"));
  }

  @Test
  public void testProviderSpecialtyMapping() {
    mapper.parse(samplePSL);

    String output = captureOutput();

    // Check that PROVIDER_SPECIALTY is mapped (this was a bug we fixed)
    assertTrue(output.contains("FEATURE: PROVIDER_SPECIALTY"));
    assertTrue(output.contains("SOURCE:") && output.contains("specialty"));
  }

  // Helper methods

  private String captureOutput() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    PrintStream old = System.out;
    System.setOut(ps);

    mapper.printLineage();

    System.out.flush();
    System.setOut(old);

    return baos.toString();
  }

  private int countOccurrences(String str, String substring) {
    int count = 0;
    int index = 0;
    while ((index = str.indexOf(substring, index)) != -1) {
      count++;
      index += substring.length();
    }
    return count;
  }

  private String loadSampleConfig() {
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
        + "#PROVIDER\n"
        + "FEATURE PROVIDER_ID, PROVIDER ID, STRING, METALINK, SEARCHABLE, INDEXED\n"
        + "FEATURE NPI, NPI, STRING, INDEXED, SEARCHABLE, NULL\n"
        + "FEATURE CARE_SITE, CARE SITE, STRING, INDEXED, SEARCHABLE, NULL\n"
        + "FEATURE PROVIDER_SPECIALTY, PROVIDER SPECIALTY, STRING, INDEXED, SEARCHABLE, NULL\n"
        + "FEATURE PROVIDER_TYPE, PROVIDER TYPE, STRING, INDEXED, SEARCHABLE, NULL\n"
        + "\n"
        + "VARIABLE DATA_SCHEMA {unifyplus_eu_atropos_poc.atropos_pre_transformed}\n"
        + "VARIABLE VOCAB_SCHEMA {unifyplus_eu_atropos_poc.atropos_pre_transformed}\n"
        + "\n"
        + "CONNECTION OPTUM {\n"
        + "  url=jdbc:test\n"
        + "}\n"
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
        + "  SORT COLUMN = *ptid*\n"
        + "  CACHE = person\n"
        + "  QUERY = SELECT\n"
        + "    ptid,\n"
        + "    region,\n"
        + "    upper(gender) as gender,\n"
        + "    upper(race) as race,\n"
        + "    upper(ethnicity) as ethnicity\n"
        + "  from $DATA_SCHEMA.optum_ehr_202508__pt_clinical\n"
        + "}\n"
        + "\n"
        + "QUERY PERSON FROM PERSON{\n"
        + "  PID = *ptid*\n"
        + "  GENDER = *gender*\n"
        + "  RACE = *race*\n"
        + "  ETHNICITY = *ethnicity*\n"
        + "  DEATH = NULL\n"
        + "  ZIP = NULL\n"
        + "  GEOGRAPHIC_ENTITY = *region*\n"
        + "  GEOGRAPHIC_ENTITY_TYPE = \"REGION\"\n"
        + "}\n"
        + "\n"
        + "#--------------------------------ICD10-----------------------------------------\n"
        + "\n"
        + "DEDUPLICATED SCHEMA ICD10 {\n"
        + "  ICD10\n"
        + "  DIAGNOSIS_DATE\n"
        + "  PROVIDER_ID\n"
        + "}\n"
        + "\n"
        + "CONNECTION CACHE_ICD10 FROM OPTUM {\n"
        + "  CACHE = icd10\n"
        + "  SORT COLUMN= *ptid*\n"
        + "  QUERY = SELECT\n"
        + "    d.ptid,\n"
        + "    d.diag_date,\n"
        + "    d.diagnosis_cd,\n"
        + "    d.provid\n"
        + "  from $DATA_SCHEMA.optum_ehr_202508__diag d\n"
        + "  WHERE d.diagnosis_cd_type = 'ICD10'\n"
        + "}\n"
        + "\n"
        + "QUERY ICD10 FROM CACHE_ICD10 {\n"
        + "  PID = *ptid*\n"
        + "  ICD10 = *diagnosis_cd*\n"
        + "  DIAGNOSIS_DATE.START = *diag_date*\n"
        + "  DIAGNOSIS_DATE.END = *diag_date*\n"
        + "  PROVIDER_ID = *provid*\n"
        + "}\n"
        + "\n"
        + "#--------------------------------ICD9-----------------------------------------\n"
        + "\n"
        + "DEDUPLICATED SCHEMA ICD9 {\n"
        + "  ICD9\n"
        + "  DIAGNOSIS_DATE\n"
        + "  PROVIDER_ID\n"
        + "}\n"
        + "\n"
        + "CONNECTION CACHE_ICD9 FROM OPTUM {\n"
        + "  CACHE = icd9\n"
        + "  SORT COLUMN= *ptid*\n"
        + "  QUERY = SELECT\n"
        + "    d.ptid,\n"
        + "    d.diag_date,\n"
        + "    d.diagnosis_cd,\n"
        + "    d.provid\n"
        + "  from $DATA_SCHEMA.optum_ehr_202508__diag d\n"
        + "  WHERE d.diagnosis_cd_type = 'ICD9'\n"
        + "}\n"
        + "\n"
        + "QUERY ICD9 FROM CACHE_ICD9 {\n"
        + "  PID = *ptid*\n"
        + "  ICD9 = *diagnosis_cd*\n"
        + "  DIAGNOSIS_DATE.START = *diag_date*\n"
        + "  DIAGNOSIS_DATE.END = *diag_date*\n"
        + "  PROVIDER_ID = *provid*\n"
        + "}\n"
        + "\n"
        + "#---------------------------------MEASUREMENT----------------------------------------\n"
        + "\n"
        + "SCHEMA MEASUREMENT {\n"
        + "  LOINC\n"
        + "  MEASUREMENT_DATE\n"
        + "  VALUE_AS_NUMBER\n"
        + "  VALUE_AS_STRING\n"
        + "  UNIT\n"
        + "}\n"
        + "\n"
        + "CONNECTION CACHE_LOINC1 FROM OPTUM {\n"
        + "  CACHE = loincs1\n"
        + "  SORT COLUMN = *ptid*\n"
        + "  QUERY = SELECT\n"
        + "    ptid,\n"
        + "    test_result,\n"
        + "    loinc,\n"
        + "    result_date AS primary_date,\n"
        + "    result_unit,\n"
        + "    result_datatype\n"
        + "  FROM $DATA_SCHEMA.optum_ehr_202508__lab\n"
        + "}\n"
        + "\n"
        + "QUERY MEASUREMENT FROM CACHE_LOINC1 {\n"
        + "  ASSERT(EQUALS(*result_datatype*, \"numeric\"))\n"
        + "  PID = *ptid*\n"
        + "  LOINC= *loinc*\n"
        + "  UNIT = *result_unit*\n"
        + "  VALUE_AS_NUMBER = *test_result*\n"
        + "  VALUE_AS_STRING = NULL\n"
        + "  MEASUREMENT_DATE = *primary_date*\n"
        + "}\n"
        + "\n"
        + "QUERY MEASUREMENT FROM CACHE_LOINC1 {\n"
        + "  ASSERT(EQUALS(*result_datatype*, \"text\"))\n"
        + "  PID = *ptid*\n"
        + "  LOINC = *loinc*\n"
        + "  UNIT = *result_unit*\n"
        + "  VALUE_AS_NUMBER = NULL\n"
        + "  VALUE_AS_STRING = *test_result*\n"
        + "  MEASUREMENT_DATE = *primary_date*\n"
        + "}\n"
        + "\n"
        + "#-------------------------------------RX-----------------------------------------\n"
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
        + "CONNECTION CACHE_RX_PRESCR FROM OPTUM {\n"
        + "  SORT COLUMN= *ptid*\n"
        + "  CACHE = rx_prescr\n"
        + "  QUERY = SELECT\n"
        + "    m.ptid,\n"
        + "    m.rxdate,\n"
        + "    m.provid,\n"
        + "    m.route,\n"
        + "    m.ndc,\n"
        + "    rx.rxnorm as rxnorm,\n"
        + "    DATE(dateadd(day, m.days_supply, m.rxdate)) as end_date\n"
        + "  from $DATA_SCHEMA.optum_ehr_202508__rx_presc m\n"
        + "    left join $DATA_SCHEMA.ndc_to_rx_v5 rx on m.ndc = rx.ndc\n"
        + "}\n"
        + "\n"
        + "QUERY RX FROM CACHE_RX_PRESCR {\n"
        + "  PID = *ptid*\n"
        + "  RX = *rxnorm*\n"
        + "  NDC = *ndc*\n"
        + "  ROUTE = *route*\n"
        + "  DRUG_EXPOSURE_DATE.START = *rxdate*\n"
        + "  DRUG_EXPOSURE_DATE.END = *end_date*\n"
        + "  DRUG_EXPOSURE_TYPE.CODE = \"RX PRESCR\"\n"
        + "  DRUG_EXPOSURE_TYPE.NAME = \"RX PRESCR\"\n"
        + "  PROVIDER_ID = *provid*\n"
        + "}\n"
        + "\n"
        + "#-----------------------------PROVIDER----------------------------\n"
        + "\n"
        + "DATASET LEVEL SCHEMA PROVIDER {\n"
        + "  PROVIDER_ID\n"
        + "  NPI\n"
        + "  CARE_SITE\n"
        + "  PROVIDER_SPECIALTY\n"
        + "  PROVIDER_TYPE\n"
        + "}\n"
        + "\n"
        + "CONNECTION CACHE_PROVIDER FROM OPTUM {\n"
        + "  SORT COLUMN = *provid*\n"
        + "  CACHE = provider\n"
        + "  QUERY = SELECT\n"
        + "    p.provid,\n"
        + "    p.specialty\n"
        + "  FROM $DATA_SCHEMA.optum_ehr_202508__prov p\n"
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
        + "#--------------------------------VOCABULARY---------------------------------------\n"
        + "\n"
        + "CONNECTION ICD9_DICT FROM OPTUM {\n"
        + "  CACHE=icd9.dict\n"
        + "  QUERY=select concept_code, concept_name FROM $DATA_SCHEMA.vocab5_icd9\n"
        + "}\n"
        + "\n"
        + "TRANSFORM FROM ICD9_DICT {\n"
        + "  SOURCE.FEATURE = \"ICD9\"\n"
        + "  SOURCE.CODE = *concept_code*\n"
        + "  TARGET.NAME = *concept_name*\n"
        + "}\n"
        + "\n"
        + "CONNECTION ICD10_DICT FROM OPTUM{\n"
        + "  CACHE = icd10.dict\n"
        + "  QUERY = select concept_code, concept_name FROM $DATA_SCHEMA.vocab5_icd10\n"
        + "}\n"
        + "\n"
        + "TRANSFORM FROM ICD10_DICT {\n"
        + "  SOURCE.FEATURE = \"ICD10\"\n"
        + "  SOURCE.CODE = *concept_code*\n"
        + "  TARGET.NAME = *concept_name*\n"
        + "}\n"
        + "\n"
        + "CONNECTION GENDER_DICT {\n"
        + "  FILE=gender.dict\n"
        + "}\n"
        + "\n"
        + "TRANSFORM FROM GENDER_DICT {\n"
        + "  SOURCE.FEATURE = \"GENDER\"\n"
        + "  TARGET.FEATURE = \"GENDER\"\n"
        + "  SOURCE.CODE = C1\n"
        + "  TARGET.CODE = C2\n"
        + "  TARGET.NAME = C2\n"
        + "}\n"
        + "\n"
        + "CONNECTION RACE_DICT {\n"
        + "  FILE=race.dict\n"
        + "}\n"
        + "\n"
        + "TRANSFORM FROM RACE_DICT {\n"
        + "  SOURCE.FEATURE = \"RACE\"\n"
        + "  TARGET.FEATURE = \"RACE\"\n"
        + "  SOURCE.CODE = C1\n"
        + "  TARGET.CODE = C2\n"
        + "  TARGET.NAME = C2\n"
        + "}\n"
        + "\n"
        + "#--------------------------------HIERARCHIES---------------------------------------\n"
        + "\n"
        + "CONNECTION RXNORM_TO_RXNORM FROM OPTUM {\n"
        + "  CACHE=rxnorm_to_rxnorm.hier\n"
        + "  QUERY = select anc_concept_code, des_concept_code FROM $VOCAB_SCHEMA.derived_rxcui_to_rxcui_map\n"
        + "}\n"
        + "\n"
        + "HIERARCHY FROM RXNORM_TO_RXNORM {\n"
        + "  CHILD.FEATURE = \"RX\"\n"
        + "  PARENT.FEATURE = \"RX\"\n"
        + "  CHILD.CODE = *des_concept_code*\n"
        + "  PARENT.CODE = *anc_concept_code*\n"
        + "}\n"
        + "\n"
        + "CONNECTION ATC_TO_RXNORM FROM OPTUM {\n"
        + "  CACHE = atc_to_rxnorm\n"
        + "  QUERY = select anc_concept_code, des_concept_code FROM $VOCAB_SCHEMA.derived_atc_to_rxcui_map\n"
        + "}\n"
        + "\n"
        + "HIERARCHY FROM ATC_TO_RXNORM {\n"
        + "  CHILD.FEATURE = \"RX\"\n"
        + "  PARENT.FEATURE = \"ATC\"\n"
        + "  CHILD.CODE = *des_concept_code*\n"
        + "  PARENT.CODE = *anc_concept_code*\n"
        + "}\n"
        + "\n"
        + "CONNECTION VISIT_TYPE_HIER {\n"
        + "  FILE=visit.hier\n"
        + "}\n"
        + "\n"
        + "HIERARCHY FROM VISIT_TYPE_HIER {\n"
        + "  CHILD.FEATURE=\"VISIT_TYPE\"\n"
        + "  PARENT.FEATURE = \"VISIT_TYPE\"\n"
        + "  CHILD.CODE=C1\n"
        + "  PARENT.CODE=C2\n"
        + "}";
  }
}