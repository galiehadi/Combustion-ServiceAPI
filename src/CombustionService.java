package id.co.smltech.energy.pjb.soket.services;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FlushModeType;
import javax.persistence.Query;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import id.co.smltech.energy.pjb.soket.utils.Utils;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

@Service
public class CombustionService {

	@Autowired
	private EntityManagerFactory emf;

	public JSONArray alarmHistory(int page, int limit) {
		JSONArray result = new JSONArray();
		int offset = page * limit;
		
	   	EntityManager em2 = emf.createEntityManager();
        em2.setFlushMode(FlushModeType.COMMIT);
		
        Query queryAlarm = em2.createNativeQuery("SELECT f_int_id, DATE_FORMAT(f_timestamp, '%d/%m/%Y %H:%i'), f_set_value, f_actual_value, f_desc FROM tb_combustion_alarm_history ORDER BY f_timestamp DESC LIMIT :limit OFFSET :offset ");
        queryAlarm.setParameter("limit",limit);
        queryAlarm.setParameter("offset",offset);
		@SuppressWarnings("unchecked")
		List<Object[]> listAlarm = queryAlarm.getResultList();
		em2.clear();
		for(Object[] objectAlarm : listAlarm){
			long alarmId = Utils.objectToLong(objectAlarm[0]);
			String date = Utils.objectToString(objectAlarm[1]);
			String setValue = Utils.objectToString(objectAlarm[2]);
			String actualValue = Utils.objectToString(objectAlarm[3]);
			String desc = Utils.objectToString(objectAlarm[4]);
		    		
			JSONObject alarm = new JSONObject();
			alarm.put("alarmId", alarmId);
			alarm.put("date", date);
			alarm.put("setValue", setValue);
			alarm.put("actualValue", actualValue);
			alarm.put("desc", desc);
			result.add(alarm);
		}
		em2.close();
		return result;
	}
	
	public JSONObject alarmDetail(long alarmId) {

		JSONObject alarm = new JSONObject();
		
		EntityManager em2 = emf.createEntityManager();
        em2.setFlushMode(FlushModeType.COMMIT);
		Query queryAlarm = em2.createNativeQuery("SELECT f_int_id, DATE_FORMAT(f_timestamp, '%d/%m/%Y %H:%i'), f_set_value, f_actual_value, f_desc "
				+ "FROM tb_combustion_alarm_history WHERE f_int_id = :alarmId ");
		queryAlarm.setParameter("alarmId",alarmId);
		@SuppressWarnings("unchecked")
		List<Object[]> listAlarm = queryAlarm.getResultList();
		em2.clear();
		if(listAlarm.size() > 0) {
			Object[] objectAlarm = listAlarm.get(0);
			String date = Utils.objectToString(objectAlarm[1]);
			String setValue = Utils.objectToString(objectAlarm[2]);
			String actualValue = Utils.objectToString(objectAlarm[3]);
			String desc = Utils.objectToString(objectAlarm[4]);
		    		
			alarm.put("alarmId", alarmId);
			alarm.put("date", date);
			alarm.put("setValue", setValue);
			alarm.put("actualValue", actualValue);
			alarm.put("desc", desc);
		}
		em2.close();
		return alarm;
	}

	public JSONObject updateAlarm(long alarmId, String description) {

		EntityManager em2 = emf.createEntityManager();
        em2.setFlushMode(FlushModeType.COMMIT);
		em2.getTransaction().begin();
	    em2.createNativeQuery("UPDATE tb_combustion_alarm_history SET f_desc = :description WHERE f_int_id = :alarmId ")
	    .setParameter("alarmId", alarmId).setParameter("description", description).executeUpdate();
	    em2.getTransaction().commit();
	    em2.clear();
	    em2.close();
	    
		JSONObject result= this.alarmDetail(alarmId);
		return result;
	}
	
	public JSONObject getIndicator() {
		EntityManager em = emf.createEntityManager();
        em.setFlushMode(FlushModeType.COMMIT);

		JSONObject combTags = getCombustionTags(em);
		JSONObject safeIndicator = getSafeIndicator(em);
		JSONObject recommendations = getRecommendations(em);
		JSONArray parameter = getParameter(em);
		JSONArray rules = getRules(em);
		em.close();

		JSONObject result = new JSONObject();
		result.merge(combTags);
		result.merge(safeIndicator);
		result.merge(recommendations);
		result.put("parameter", parameter);
		result.put("rules", rules);
		return result;
	}
	
	private JSONArray getRules(EntityManager em) {
		JSONArray rules = new JSONArray();
		Query queryRules = em.createNativeQuery("SELECT tsrh.f_rule_hdr_id, tsrh.f_rule_descr FROM tb_combustion_rules_hdr tsrh " +
				"WHERE tsrh.f_is_active = 1 ORDER BY tsrh.f_rule_hdr_id");
		@SuppressWarnings("unchecked")
		List<Object[]> dataRules = queryRules.getResultList();
		em.clear();
		
		for(Object[] objectRules : dataRules){
			int ruleId = Utils.objectToInteger(objectRules[0]);
			String ruleName = Utils.objectToString(objectRules[1]);
    		
    		JSONObject rule = new JSONObject();
    		rule.put("id", ruleId);
    		rule.put("label", ruleName);
    		rules.add(rule);
		}
		return rules;
	}
	
	private JSONArray getParameter(EntityManager em2) {
		JSONArray parameters = new JSONArray();
		
		Query queryParameter = em2.createNativeQuery("SELECT tsp.f_label, tsp.f_parameter_descr, tsp.f_default_value, tsp.f_parameter_id " +
				"FROM tb_combustion_parameters tsp " + 
				"WHERE tsp.f_is_active = 1 ORDER BY tsp.f_parameter_id ASC");
		@SuppressWarnings("unchecked")
		List<Object[]> dataParameter = queryParameter.getResultList();
		em2.clear();
		
		for(Object[] objectParameter : dataParameter){
			String label= Utils.objectToString(objectParameter[0]);	
			String description= Utils.objectToString(objectParameter[1]);
			Double value = Utils.objectToDouble(objectParameter[2]);
			int paramId	= Utils.objectToInteger(objectParameter[3]);
    		
    		JSONObject params = new JSONObject();
    		params.put("id", paramId);
    		params.put("label", description);
    		
    		if(label.startsWith("control_mode")) {
    			if(value == 1) {
        			params.put("value", "AUTO (PLC)");	
    			}else if(value == 2) {
        			params.put("value", "REMOTE (DCS)");	
    			}else if(value == 3) {
        			params.put("value", "LOCAL (HMI)");	
    			}else {
        			params.put("value", "BAT");	
    			}
    		}else {
    			params.put("value", value);	
    		}
    		parameters.add(params);
		}
		
		return parameters;
	}
	
	private JSONObject getCombustionTags(EntityManager em) {
		JSONObject combTags = new JSONObject();
		JSONObject tags = new JSONObject();

        Query combTagsQuery = em.createNativeQuery("SELECT REPLACE(LOWER(tcc.f_constraint), ' ', '_') as f_constraint, IFNULL(tcr.f_value , tcc.f_value) AS f_value, tcc.f_type "
        		+ "FROM tb_comb_constraint tcc "
        		+ "LEFT JOIN tb_combustion_raw tcr ON tcc.f_address_no_read = tcr.f_address_no "
        		+ "WHERE f_int_id IN (44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79)");
        @SuppressWarnings("unchecked")
		List<Object[]> listCombTags = combTagsQuery.getResultList();
		em.clear();

		for (Object[] tag : listCombTags) {
			tags.put(Utils.objectToString(tag[0]), String.format("%.1f", Utils.objectToDouble(tag[1]))+" "+Utils.objectToString(tag[2]));
		}

		combTags.put("comb_tags", tags);
		return combTags;
	}

	private JSONObject getSafeIndicator(EntityManager em) {
		JSONObject safeIndicator = new JSONObject();

        Query safeIndicatorQuery = em.createNativeQuery("SELECT f_constraint, f_value from tb_comb_constraint tcc WHERE tcc.f_int_id in (1, 2, 3)");
        @SuppressWarnings("unchecked")
		List<Object[]> listSafeIndicator = safeIndicatorQuery.getResultList();
		em.clear();

		for (Object[] indicator : listSafeIndicator) {
			String name = Utils.objectToString(indicator[0]).toLowerCase();
			Double value = Utils.objectToDouble(indicator[1]);
			safeIndicator.appendField(name, value);
		}
		
		Query queryLastRecom = em.createNativeQuery("SELECT DATE_FORMAT(MAX(ts), '%d/%m/%Y %H:%i:%s') FROM tb_combustion_model_generation");
		@SuppressWarnings("unchecked")
		List<Object[]> listLastRecom = queryLastRecom.getResultList();
		em.clear();
		
		String lastRecommendation = "";
		if(listLastRecom.size() > 0) {
			lastRecommendation = Utils.objectToString(listLastRecom.get(0));
		}

		safeIndicator.put("last_recommendation", lastRecommendation);
		return safeIndicator;
	}

	private JSONObject getRecommendations(EntityManager em) {
		JSONObject recommendations = new JSONObject();
		JSONArray recommendationArr = new JSONArray();

        Query recommendationQuery = em.createNativeQuery("SELECT tcmg.ts as `timestamp`, tcmg.tag_name as `desc`, tcmg.enable_status as status, (tcmg.value - tcmg.bias_value) as currentValue, tcmg.bias_value as setValue, tcc.f_type, tcmg.value as targetValue FROM tb_combustion_model_generation tcmg "
        		+ "left join tb_comb_constraint tcc on tcmg.tag_name = tcc.f_constraint "
        		+ "LEFT JOIN tb_combustion_raw tcr on tcc.f_address_no_read = tcr.f_address_no "
        		+ "JOIN (SELECT DISTINCT process_id process_id FROM tb_combustion_model_generation tcmg2 ORDER BY process_id DESC LIMIT 2, 1) a ON tcmg.process_id >= a.process_id "
        		+ "WHERE tcmg.tag_name NOT IN ('Excess O2 A', 'Excess O2 B') "
        		+ "ORDER BY tcmg.process_id DESC, tcmg.tag_name ASC");
        @SuppressWarnings("unchecked")
		List<Object[]> recommendationList = recommendationQuery.getResultList();
		em.clear();

		for (Object[] recommendation : recommendationList) {
			String type = Utils.objectToString(recommendation[5]);
			recommendationArr.add(
				new JSONObject().appendField("timestamp", Utils.objectToString(recommendation[0]))
					.appendField("desc", Utils.objectToString(recommendation[1]))
					.appendField("status", Utils.objectToInteger(recommendation[2]))
					.appendField("currentValue", String.format("%.2f", Utils.objectToDouble(recommendation[3]))+" "+type)
					.appendField("setValue", String.format("%.2f", Utils.objectToDouble(recommendation[4]))+" "+type)
					.appendField("targetValue", String.format("%.2f", Utils.objectToDouble(recommendation[6]))+" "+type));
		}

		recommendations.appendField("recommendations", recommendationArr);

		return recommendations;
	}
	
	public JSONObject getParameter(int parameterId) {
		JSONObject result = new JSONObject();
		EntityManager em = emf.createEntityManager();
        em.setFlushMode(FlushModeType.COMMIT);
		Query queryParameter = em.createNativeQuery("SELECT tsp.f_parameter_id, tsp.f_parameter_descr, tsp.f_default_value " +
					"FROM tb_combustion_parameters tsp WHERE tsp.f_parameter_id = :parameterId AND tsp.f_is_active = 1"); 
		queryParameter.setParameter("parameterId",parameterId);
	    @SuppressWarnings("unchecked")
		List<Object[]> listParameter = queryParameter.getResultList();
	    em.clear();
	    
	    if(listParameter.size() > 0) {
		    result.put("id", listParameter.get(0)[0]);
		    result.put("label", listParameter.get(0)[1]);
		    result.put("value", listParameter.get(0)[2]);
	    }
	    em.close();
		return result;
	}
	
	public JSONObject saveParameter(int parameterId, String description, String defaultValue) {

		JSONObject result = new JSONObject();
		EntityManager em = emf.createEntityManager();
        em.setFlushMode(FlushModeType.COMMIT);
		Query queryParameter = em.createNativeQuery("SELECT tsp.f_parameter_id, tsp.f_parameter_descr, tsp.f_default_value " +
				"FROM tb_combustion_parameters tsp WHERE tsp.f_parameter_id = :parameterId AND tsp.f_is_active = 1"); 
		queryParameter.setParameter("parameterId",parameterId);
	    @SuppressWarnings("unchecked")
		List<Object[]> listParameter = queryParameter.getResultList();
	    em.clear();
	    
	    if(listParameter.size() > 0) {
	    	em.getTransaction().begin();
		    em.createNativeQuery("UPDATE tb_combustion_parameters SET f_default_value = :defaultValue WHERE f_parameter_id = :parameterId ")
		    .setParameter("defaultValue", defaultValue).setParameter("parameterId", parameterId).executeUpdate();
		    em.getTransaction().commit();
		    em.clear();
		    
		    result.put("id", listParameter.get(0)[0]);
		    result.put("label", listParameter.get(0)[1]);
		    result.put("value", listParameter.get(0)[2]);
	    }else {
			return result;
		}
	    
    	result.put("id", parameterId);
    	result.put("label", description);
    	result.put("value", defaultValue);
		return result;
	}
	
	public JSONObject getRule(int ruleId) {
		JSONObject result = new JSONObject();
	   	JSONArray detail = new JSONArray();
	   	EntityManager em = emf.createEntityManager();
        em.setFlushMode(FlushModeType.COMMIT);
        
		Query queryRule = em.createNativeQuery("SELECT tsrh .f_rule_hdr_id, tsrh .f_rule_descr " +
					"FROM tb_combustion_rules_hdr tsrh WHERE tsrh.f_rule_hdr_id = :ruleId AND tsrh.f_is_active=1 "); 
		queryRule.setParameter("ruleId",ruleId);
		@SuppressWarnings("unchecked")
		List<Object[]> listRule = queryRule.getResultList();
		em.clear();
		
		if(listRule.size() > 0) {
			result.put("id", listRule.get(0)[0]);
			result.put("label", listRule.get(0)[1]);
			
			Query queryDetailRules = em.createNativeQuery("SELECT tsrd.f_tag_sensor, tsrd.f_bracket_open, tsrd.f_bracket_close, tsrd.f_sequence, tsrd.f_rule_dtl_id " +
			"FROM tb_combustion_rules_dtl tsrd WHERE tsrd.f_rule_hdr_id = :ruleId AND tsrd.f_is_active = 1 ORDER BY tsrd.f_sequence");
			queryDetailRules.setParameter("ruleId",ruleId);
			@SuppressWarnings("unchecked")
			List<Object[]> listDetailRules = queryDetailRules.getResultList();
			em.clear();
			
			for(Object[] objectDetailRules : listDetailRules){
				String tagSensor = Utils.objectToString(objectDetailRules[0]);
				String bracketOpen = Utils.objectToString(objectDetailRules[1]);
				String bracketClose = Utils.objectToString(objectDetailRules[2]);
				int sequence = Utils.objectToInteger(objectDetailRules[3]);
	    		
	    		JSONObject detailRule = new JSONObject();
	    		detailRule.put("sequence", sequence);
	    		detailRule.put("tagSensor", tagSensor);
	    		detailRule.put("bracketOpen", bracketOpen);
	    		detailRule.put("bracketClose", bracketClose);
	    		detail.add(detailRule);
			}
			result.put("detailRule", detail);
		}
		em.close();
		return result;
	}
	
	public void deleteDetailRule(int hdrId) {
		EntityManager em = emf.createEntityManager();
        em.setFlushMode(FlushModeType.COMMIT);
		em.getTransaction().begin();
	    em.createNativeQuery("DELETE FROM tb_combustion_rules_dtl WHERE f_rule_hdr_id = :hdrId ")
	    .setParameter("hdrId", hdrId).executeUpdate();  
	    em.getTransaction().commit();
	    em.close();
	}

	public void saveDetailRule(int ruleHeaderId, String bracketOpen, String bracketClose, String tagSensor, int sequence) {
		EntityManager em2 = emf.createEntityManager();
        em2.setFlushMode(FlushModeType.COMMIT);
		em2.getTransaction().begin();
		em2.createNativeQuery("INSERT INTO "
				+ "tb_combustion_rules_dtl(f_rule_hdr_id, f_rule_descr, f_tag_sensor, f_rules, f_operator, f_unit, f_limit_high, f_limit_low, f_sequence, f_bracket_open, f_bracket_close, f_is_active, f_updated_at) "
				+ "VALUES( :ruleHeaderId , '', :tagSensor, '', '', '', '', '', :sequence, :bracketOpen, :bracketClose, 1, NOW())")
	    .setParameter("ruleHeaderId", ruleHeaderId)
	    .setParameter("tagSensor", tagSensor)
	    .setParameter("sequence", sequence)
	    .setParameter("bracketOpen", bracketOpen)
	    .setParameter("bracketClose", bracketClose)
	    .executeUpdate();  
	    em2.getTransaction().commit();
	    em2.close();
	}
}
