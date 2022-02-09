package id.co.smltech.energy.pjb.soket.apis;

import java.util.List;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import id.co.smltech.energy.pjb.soket.dto.Response;
import id.co.smltech.energy.pjb.soket.services.CombustionService;
import id.co.smltech.energy.pjb.soket.services.FrontService;
import id.co.smltech.energy.pjb.soket.utils.ResponseMessageUtils;
import id.co.smltech.energy.pjb.soket.utils.Utils;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

@RestController
@RequestMapping("/combustion")
public class CombustionAPI {
	@Autowired
	private CombustionService combService;
	@Autowired
	FrontService frontendService;

	@GetMapping("alarm-history")
	public ResponseEntity<Response> getAlarmHistory(@RequestParam(required = false) String limit, @RequestParam(required = false) String page) {
		int climit = 50;
		int cpage = 0;
		if(limit != null) {
			climit = Utils.objectToInteger(limit);
		}
		if(page != null) {
			cpage = Utils.objectToInteger(page);
		}
		Response response = new Response();
		response.setObject(combService.alarmHistory(cpage, climit));
    	response.setMessage(ResponseMessageUtils.getSuccess()); 
    	response.setLimit(climit);
    	response.setPage(cpage);
    	return ResponseEntity.ok(response);
		
	}
	
	@GetMapping("detail/alarm-history/{alarmId}")
	public ResponseEntity<Response> getDetailAlarmHistory(@PathVariable long alarmId) {
		Response response = new Response();
		response.setObject(combService.alarmDetail(alarmId));
    	response.setMessage(ResponseMessageUtils.getSuccess()); 
    	response.setLimit(1);
    	response.setPage(0);
    	return ResponseEntity.ok(response);
		
	}
	
	@PostMapping("update/alarm-history")
	public ResponseEntity<Response> updateAlarm(@Valid @RequestBody Alarm alarm){
		Response response 	= new Response();
		JSONObject result 	= new JSONObject();
		result = combService.updateAlarm(alarm.getAlarmId(), alarm.getDesc());
		response.setMessage(ResponseMessageUtils.getSuccess());
    	response.setObject(result);
		return ResponseEntity.ok(response);
	}
	
	@GetMapping("indicator")
	public ResponseEntity<Response> getIndicator() {
		Response response = new Response();
		response.setObject(combService.getIndicator());
    	response.setMessage(ResponseMessageUtils.getSuccess());
    	return ResponseEntity.ok(response);
	}
	
	@GetMapping("parameter/{parameterId}")
	public ResponseEntity<Response> getParameter(@PathVariable int parameterId) {
		Response response = new Response();
		JSONObject result = combService.getParameter(parameterId);
		response.setObject(result);
    	response.setMessage(ResponseMessageUtils.getSuccess());
    	return ResponseEntity.ok(response);
	}
	
	@PreAuthorize("hasRole('ROLE_ENGINEER')")
	@PostMapping("parameter")
	public ResponseEntity<Response> saveParameter(@Valid @RequestBody JSONObject parameter){
		Response response = new Response();
		int parameterId = 0;
		String parameterDescr = "";
		String defaultValue = "";
		try {
			parameterId = Utils.objectToInteger(parameter.get("id"));
			parameterDescr = Utils.objectToString(parameter.get("label"));
			defaultValue = Utils.objectToString(parameter.get("value"));
		}catch (Exception e) {
			response.setMessage("Format Not Acceptable");
    		return new ResponseEntity<Response>(response, HttpStatus.NOT_ACCEPTABLE);
		}
		
		JSONObject result = combService.saveParameter(parameterId, parameterDescr, defaultValue);
		response.setObject(result);
		return ResponseEntity.ok(response);
	}
	
	@GetMapping("tags/rule")
	public ResponseEntity<Response> getRuleTags() {
		Response response = new Response();
		
		JSONArray result = frontendService.getRuleTags();
		response.setObject(result);
    	response.setMessage(ResponseMessageUtils.getSuccess());
    	return ResponseEntity.ok(response);
		
	}
	
	@GetMapping("rule/{ruleId}")
	public ResponseEntity<Response> getRule(@PathVariable int ruleId) {
		Response response = new Response();
		JSONObject result = combService.getRule(ruleId);
		response.setObject(result);
    	response.setMessage(ResponseMessageUtils.getSuccess());
    	return ResponseEntity.ok(response);
	}
	
	@PreAuthorize("hasRole('ROLE_ENGINEER')")
	@PostMapping("rule")
	public ResponseEntity<Response> saveRule(@Valid @RequestBody CombutionRules rule){
		Response response 	= new Response();
		JSONObject result 	= new JSONObject();
		String logic		= "";
		
		List<CombustionRuleDetail> detailRule = rule.getCombustionRuleDetail();
		for (int i = 0 ; i < detailRule.size(); i++) {
			CombustionRuleDetail detail 	= detailRule.get(i);
			String bracketOpen	= detail.getBracketOpen();
			String bracketClose	= detail.getBracketClose();
			logic = logic + bracketOpen + "0" + bracketClose;
		}
		
		logic = logic.replace("OR", "||"); 
		logic = logic.replace("AND", "&&"); 
		logic = logic.replace("=", "==");
		logic = logic.replace("ln(", "Math.log("); 
		
		ScriptEngineManager manager = new ScriptEngineManager();
		ScriptEngine engine = manager.getEngineByName("js");

		try {
			engine.eval(logic);
		} catch (Exception e) {
			response.setMessage(e.getMessage());
    		return new ResponseEntity<Response>(response, HttpStatus.NOT_ACCEPTABLE);
	    }
		int ruleHeaderId	= rule.getId();
		combService.deleteDetailRule(ruleHeaderId);
		
		for (int i = 0 ; i < detailRule.size(); i++) {
			CombustionRuleDetail detail 	= detailRule.get(i);
			String bracketOpen	= detail.getBracketOpen();
			String bracketClose	= detail.getBracketClose();
			String tagSensor	= detail.getTagSensor();
			int sequence		= detail.getSequence();
			combService.saveDetailRule(ruleHeaderId, bracketOpen, bracketClose, tagSensor, sequence);
		}
		
		result = frontendService.getRule(rule.getId());
		response.setObject(result);
		return ResponseEntity.ok(response);
	}
}
