import java.util.Arrays;
import java.util.List;

import com.sap.hana.cvapi.basemodel.base.Cardinality;
import com.sap.hana.cvapi.basemodel.base.JoinType;
import com.sap.hana.cvapi.basemodel.base.PrivilegeType;
import com.sap.hana.cvapi.bimodel.calculcation.AttributeMapping;
import com.sap.hana.cvapi.bimodel.calculcation.CalculationScenario;
import com.sap.hana.cvapi.bimodel.calculcation.CalculationViewType;
import com.sap.hana.cvapi.bimodel.calculcation.CalculationViews;
import com.sap.hana.cvapi.bimodel.calculcation.DataSource;
import com.sap.hana.cvapi.bimodel.calculcation.DataSources;
import com.sap.hana.cvapi.bimodel.calculcation.Input;
import com.sap.hana.cvapi.bimodel.calculcation.JoinAttribute;
import com.sap.hana.cvapi.bimodel.calculcation.JoinView;
import com.sap.hana.cvapi.bimodel.calculcation.ProjectionView;
import com.sap.hana.cvapi.bimodel.calculcation.ScenarioRoot;
import com.sap.hana.cvapi.bimodel.calculcation.ViewAttribute;
import com.sap.hana.cvapi.bimodel.calculcation.ViewAttributes;
import com.sap.hana.cvapi.bimodel.cube.AggregationType;
import com.sap.hana.cvapi.bimodel.cube.BaseMeasure;
import com.sap.hana.cvapi.bimodel.cube.BaseMeasures;
import com.sap.hana.cvapi.bimodel.cube.MeasureGroup;
import com.sap.hana.cvapi.bimodel.cube.MeasureType;
import com.sap.hana.cvapi.bimodel.dataFoundation.Attribute;
import com.sap.hana.cvapi.bimodel.dataFoundation.Attributes;
import com.sap.hana.cvapi.bimodel.dataFoundation.CalculatedAttributes;
import com.sap.hana.cvapi.bimodel.dataFoundation.ColumnMapping;
import com.sap.hana.cvapi.bimodel.calculcation.DataCategory;
import com.sap.hana.cvapi.datamodel.type.SemanticType;

public class CreateCaculationView {
	private static final String CAL_SCENARIO_ID = "yikai_demo.db.models::PERFORMANCE_SALARIES_BY_JAVA";
	private static final String JOIN_ID = "ID";

	private static final String PERFORMANCE_TABLE = "yikai_demo.db.data::PERFORMANCE";
	private static final String SALARY_TABLE = "yikai_demo.db.data::SALARY";

	private static final String ID_COLUMN = "ID";
	private static final String SALARY_COLUMN = "SALARY";
	private static final String START_YEAR_COLUMN = "START_YEAR";
	private static final String GENDER_COLUMN = "GENDER";
	private static final String REGION_COLUMN = "REGION";
	private static final String T_LEVEL_COLUMN = "T-LEVEL";
	private static final String EVLUATION_RATING_COLUMN = "EVALUATION_RATING";
	private static final String REPORTS_TO_COLUMN = "REPORTS_TO";
	private static final String FEEDBACK_COMMNENTS_COLUMN = "FEEDBACK_COMMENT";
	private static final String SATISFACTION_INDEX_COLUMN = "SATISFACTION_INDEX";

	private static final String JOIN_VIEW_NAME = "Join_1";
	private static final String SALARY_PROJ_NAME = "yikai_demo.db.data::SALARY";
	private static final String PERFORMANCE_PROJ_NAME = "yikai_demo.db.data::PERFORMANCE";
	
	public static void main(final String[] args) {
		System.out.println(createRoot().toXMLWithXSDValidation());
	}

	public static ScenarioRoot createRoot() {
		// Create the data sources of the view --> all accessed tables
		final DataSources dataSources = new DataSources();
		dataSources.addDataSource(createDataSource(SALARY_TABLE));
		dataSources.addDataSource(createDataSource(PERFORMANCE_TABLE));

		// Create the semantic attributes
		final Attributes semanticAttributes = new Attributes();
		semanticAttributes.addAttribute(createAttribute(ID_COLUMN, 1, JOIN_VIEW_NAME));
		semanticAttributes.addAttribute(createAttribute(START_YEAR_COLUMN, 3, JOIN_VIEW_NAME));
		semanticAttributes.addAttribute(createAttribute(GENDER_COLUMN, 4, JOIN_VIEW_NAME));
		semanticAttributes.addAttribute(createAttribute(REGION_COLUMN, 5, JOIN_VIEW_NAME));
		semanticAttributes.addAttribute(createAttribute(T_LEVEL_COLUMN, 6, JOIN_VIEW_NAME));
		semanticAttributes.addAttribute(createAttribute(REPORTS_TO_COLUMN, 8, JOIN_VIEW_NAME));
		semanticAttributes.addAttribute(createAttribute(FEEDBACK_COMMNENTS_COLUMN, 9, JOIN_VIEW_NAME));

		// Create the semantic measures
		final BaseMeasures semanticMeasures = new BaseMeasures();
		semanticMeasures.addMeasure(createMeasure(SALARY_COLUMN, 2, JOIN_VIEW_NAME));
		semanticMeasures.addMeasure(createMeasure(EVLUATION_RATING_COLUMN, 7, JOIN_VIEW_NAME));
		semanticMeasures.addMeasure(createMeasure(SATISFACTION_INDEX_COLUMN, 10, JOIN_VIEW_NAME));

		// Semantics node
		final MeasureGroup logicalModel = new MeasureGroup();
		logicalModel.setId(JOIN_VIEW_NAME);
		logicalModel.setAttributes(semanticAttributes);
		logicalModel.setBaseMeasures(semanticMeasures);
		logicalModel.setCalculatedAttributes(new CalculatedAttributes());

		final CalculationViews calculationViews = new CalculationViews();
		calculationViews.addCalculationView(createJoinView());
		// calculationViews.addCalculationView(createProductProjectionView());
		// calculationViews.addCalculationView(createSalesProjectionView());

		final CalculationScenario calcScenario = new CalculationScenario();
		calcScenario.setId(CAL_SCENARIO_ID);
		calcScenario.setApplyPrivilegeType(PrivilegeType.NONE);
		calcScenario.setOutputViewType(CalculationViewType.AGGREGATION);
		calcScenario.setDataCategory(DataCategory.CUBE);
		calcScenario.setLogicalModel(logicalModel);
		calcScenario.setCalculationViews(calculationViews);
		calcScenario.setDataSources(dataSources);

		final ScenarioRoot root = new ScenarioRoot();
		root.setScenario(calcScenario);
		return root;
	}

	// ================================================================================
	// Views
	// ================================================================================

//	private static ProjectionView createSalesProjectionView() {
//		return createProjection(SALES_PROJ_NAME, PERFORMANCE_TABLE, Arrays.asList(ID_COLUMN, PRODUCTID_COLUMN, AMOUNT_COLUMN, DETAIL_COLUMN));
//	}
//
//	private static ProjectionView createProductProjectionView() {
//		return createProjection(PRODUCT_PROJ_NAME, SALARY_TABLE, Arrays.asList(ID_COLUMN, PRODUCTNAME_COLUMN, PRICE_COLUMN));
//	}

	private static ProjectionView createProjection(final String name, final String dataSource, final List<String> col) {
		final Input input = new Input();
		final ViewAttributes viewAttributes = new ViewAttributes();

		input.setNode(dataSource);
		col.forEach(colN -> {
			viewAttributes.addViewAttribute(createViewAttribute(colN));
			input.addMapping(createMapping(colN, colN));
		});

		final ProjectionView projectionView1 = new ProjectionView();
		projectionView1.setId(name);
		projectionView1.setViewAttributes(viewAttributes);
		projectionView1.addInput(input);
		return projectionView1;
	}

	private static JoinView createJoinView() {
		final ViewAttributes viewAttributes = new ViewAttributes();
		viewAttributes.addViewAttribute(createViewAttribute(ID_COLUMN, AggregationType.SUM));
		viewAttributes.addViewAttribute(createViewAttribute(SALARY_COLUMN, AggregationType.SUM));
		viewAttributes.addViewAttribute(createViewAttribute(START_YEAR_COLUMN, AggregationType.SUM));
		viewAttributes.addViewAttribute(createViewAttribute(GENDER_COLUMN));
		viewAttributes.addViewAttribute(createViewAttribute(REGION_COLUMN));
		viewAttributes.addViewAttribute(createViewAttribute(T_LEVEL_COLUMN));
		viewAttributes.addViewAttribute(createViewAttribute(EVLUATION_RATING_COLUMN, AggregationType.SUM));
		viewAttributes.addViewAttribute(createViewAttribute(REPORTS_TO_COLUMN, AggregationType.SUM));
		viewAttributes.addViewAttribute(createViewAttribute(FEEDBACK_COMMNENTS_COLUMN));
		viewAttributes.addViewAttribute(createViewAttribute(SATISFACTION_INDEX_COLUMN, AggregationType.SUM));

		//final ViewAttribute joinViewAttribute = new ViewAttribute();
		//joinViewAttribute.setId(JOIN_ID);
		//joinViewAttribute.setHidden(Boolean.TRUE);
		//viewAttributes.addViewAttribute(joinViewAttribute);

		final Input salaryInput = new Input();
		salaryInput.setNode(SALARY_PROJ_NAME);
		salaryInput.addMapping(createMapping(ID_COLUMN, JOIN_ID));
		salaryInput.addMapping(createMapping(SALARY_COLUMN, SALARY_COLUMN));
		salaryInput.addMapping(createMapping(START_YEAR_COLUMN, START_YEAR_COLUMN));
		salaryInput.addMapping(createMapping(GENDER_COLUMN, GENDER_COLUMN));
		salaryInput.addMapping(createMapping(REGION_COLUMN, REGION_COLUMN));
		salaryInput.addMapping(createMapping(T_LEVEL_COLUMN, T_LEVEL_COLUMN));

		final Input performanceInput = new Input();
		performanceInput.setNode(PERFORMANCE_PROJ_NAME);
		performanceInput.addMapping(createMapping(ID_COLUMN, JOIN_ID));
		performanceInput.addMapping(createMapping(EVLUATION_RATING_COLUMN, EVLUATION_RATING_COLUMN));
		performanceInput.addMapping(createMapping(REPORTS_TO_COLUMN, REPORTS_TO_COLUMN));
		performanceInput.addMapping(createMapping(FEEDBACK_COMMNENTS_COLUMN, FEEDBACK_COMMNENTS_COLUMN));
		performanceInput.addMapping(createMapping(SATISFACTION_INDEX_COLUMN, SATISFACTION_INDEX_COLUMN));

		final JoinAttribute joinAttribute = new JoinAttribute();
		joinAttribute.setName(JOIN_ID);

		final JoinView joinView = new JoinView();
		joinView.setId(JOIN_VIEW_NAME);
		joinView.setViewAttributes(viewAttributes);
		joinView.addInput(performanceInput);
		joinView.addInput(salaryInput);
		joinView.setJoinType(JoinType.INNER);
		joinView.setCardinality(Cardinality.C1_1);
		joinView.addJoinAttribute(joinAttribute);

		return joinView;
	}

	// ================================================================================
	// Helper
	// ================================================================================

	private static AttributeMapping createMapping(final String source, final String target) {
		final AttributeMapping mapping = new AttributeMapping();
		mapping.setSource(source);
		mapping.setTarget(target);
		return mapping;
	}

	private static ViewAttribute createViewAttribute(final String id) {
		final ViewAttribute viewAttribute = new ViewAttribute();
		viewAttribute.setId(id);
		return viewAttribute;
	}

	private static ViewAttribute createViewAttribute(final String id, final AggregationType aggregationType) {
		final ViewAttribute viewAttribute = new ViewAttribute();
		viewAttribute.setId(id);
		viewAttribute.setAggregationType(aggregationType);
		return viewAttribute;
	}

	private static BaseMeasure createMeasure(final String name, final int order, final String co) {
		final BaseMeasure measure = new BaseMeasure();
		measure.setId(name);
		measure.setOrder(Integer.valueOf(order));
		measure.setMeasureType(MeasureType.SIMPLE);
		measure.setAggregationType(AggregationType.AVG);
		measure.setMeasureMapping(createColumnMapping(name, co));
		return measure;
	}

	private static ColumnMapping createColumnMapping(final String name, final String co) {
		final ColumnMapping columnMapping = new ColumnMapping();
		columnMapping.setColumnObjectName(co);
		columnMapping.setColumnName(name);
		return columnMapping;
	}

	private static Attribute createAttribute(final String name, final int order, final String co) {
		final Attribute attribute1 = new Attribute();
		attribute1.setId(name);
		attribute1.setOrder(Integer.valueOf(order));
		//attribute1.setSemanticType(SemanticType.EMPTY);
		attribute1.setKeyMapping(createColumnMapping(name, co));
		return attribute1;
	}

	private static DataSource createDataSource(final String tableName) {
		final DataSource salesDataSource = new DataSource();
		salesDataSource.setId(tableName);
		salesDataSource.setResourceUri(tableName);
		return salesDataSource;
	}
}
