//
//  Listener.cpp
//  NapiCpp
//
//  Created by Hanieh Bastani on 2016-05-01.
//  Copyright © 2016 Hanieh Bastani. All rights reserved.
//

#include "Listener.h"
#include <mutex>
#include <condition_variable>
#include <vector>
#include "napi.h"
#include "NymiProvision.h"
#include "TransientNymiBandInfo.h"

namespace PrivateListener {

    const bool success = true;
    const bool failure = false;
    const napiError noErr = { "", {} };
    
    //variables, and their getters and setters
    std::atomic<bool> quit{ false };
    void setQuit(bool _quit) {
        quit.store(_quit);
    }

    std::mutex finishMtx;
    std::condition_variable finishCv;
    std::mutex* getFinishMtx(){ return &finishMtx; }
    std::condition_variable* getFinishCv(){ return &finishCv; }

    bool apiFinish;
    void setApiFinish(bool _apiFinish){ apiFinish = _apiFinish; }
    bool getApiFinish(){ return apiFinish; }
    
	NymiApi* nApi = nullptr;
    agreementCallback onAgreement = nullptr;
    newProvisionCallback onProvision = nullptr;
	provisionChangeCallback onProvisionChange = nullptr;
    errorCallback onError = nullptr;
    getProvisionsCallback getProvisionList = nullptr;
    onStartStopProvisioning onProvisionModeChange = nullptr;
    onNymiBandFoundStatusChange onFoundChange = nullptr;
    onNymiBandPresenceChange onPresenceChange = nullptr;
    onNotificationsGetState onNotificationsGet = nullptr;
        
	void setNAPI(NymiApi* napi) { nApi = napi; }
    void setOnAgreement(agreementCallback _onAgreement){ onAgreement = _onAgreement; }
    void setOnProvision(newProvisionCallback _onProvision){ onProvision = _onProvision; }
	void setOnProvisionChange(provisionChangeCallback _onProvisionChange) { onProvisionChange = _onProvisionChange; }
    void setOnError(errorCallback _onError){ onError = _onError; }
    void setProvisionList(getProvisionsCallback _onProvisionList){ getProvisionList = _onProvisionList; }
    void setOnProvisionModeChange(onStartStopProvisioning _onProvisionModeChange){ onProvisionModeChange = _onProvisionModeChange; }
    void setOnFoundChange(onNymiBandFoundStatusChange _onFoundChange){ onFoundChange = _onFoundChange; }
    void setOnPresenceChange(onNymiBandPresenceChange _onPresenceChange){ onPresenceChange = _onPresenceChange; }
    void setOnNotificationsGet(onNotificationsGetState _onNotificationGet){ onNotificationsGet = _onNotificationGet; }


    void waitForMessage() {
        
        while (!quit.load()) {
            char message[2048];
			unsigned long long reqbufsize = 0;
			//this is a blocking call. Returns only if napi has sent a message or the API is terminated
#ifdef _WIN32
			napi::GetOutcome res = nApi->get(&message[0], 2048, &reqbufsize);
#else
			napi::GetOutcome res = napi::get(&message[0], 2048, &reqbufsize);
#endif
            if (res == napi::GetOutcome::okay) {
                std::cout << "received message: " << message << std::endl;
                
                nljson jobj = nljson::parse(message);
                
                if (!wellConstructedJson(jobj)){
					std::cout << "ERROR: Received malformed JSON object from NAPI. Fatal - terminating" << std::endl;
#ifdef _WIN32
                    nApi->terminate();
#else
                    napi::terminate();
#endif
					exit(1);
                }
                
                //handle any errors
                nljson::iterator jit;
                if (hasKey(jobj,{"errors"},jit) || isKeyValue(jobj,{"successful"},jit,false)){
						handleNapiError(jobj);
                    continue;
                }
                
                //delegate to proper op handler
				std::vector<std::string> op;
				if (getOperation(jobj, op)) {
					std::string operation = op[0];
                    //call operation handler for this operation
                    opHandlerType::const_iterator oit;
                    if ((oit = opHandler.find(operation)) != opHandler.end()) {
                        oit->second(jobj);	//call the function for this operation
                    }
                }
            }
        }
		std::cout << "Exiting listener thread" << std::endl;
    }

    //some utility functions
    //----------------------
    bool getExchange(nljson &jobj, std::string &exchange, bool errorIfNoExchange){
        
        exchange = "";  //reset
        
        nljson::iterator jit;
        if (!hasKey(jobj, {"exchange"}, jit)){
            if (errorIfNoExchange) {
                std::string errMsg = "Could not find JSON field \"exchange\" in the JSON obj:\n";
                errMsg += jobj.dump();
                napiError err { errMsg, {} };
                onError(err);
            }
            return false;
        }
        
        exchange = jit.value();
        return true;
    }
    
    bool getPid(nljson &jobj, std::string &pid){
        
        pid = "";  //reset
        
        nljson::iterator jit;
        if (hasKey(jobj, {"request","pid"}, jit)){
            pid = jit.value();
            return true;
        }
        
        return false;
    }
    
    bool getPid(nljson &jobj, std::string &pid, napiError &nErr){
        
        if (!getPid(jobj,pid)){
        
            std::string errMsg = "Could not find JSON field \"pid\" in the JSON obj:\n";
            errMsg += jobj.dump();
            nErr = { errMsg, {} };
            return false;
        }
        return true;
    }
    
    napiError genMissingJsonKeyErr(std::string key, nljson &jobj){
        
        std::string errMsg = "Could not find JSON field \"" + key + "\" in the JSON obj:\n";
        errMsg += jobj.dump();
        napiError nErr = { errMsg, {} };
        
        return nErr;
    }

	bool getOperation(nljson jobj, std::vector<std::string> &op) {
		nljson::iterator jit;
		if (hasKey(jobj, { "path" }, jit)) {
			std::string path = jit.value();
			std::stringstream ss(path);
			while (ss.good())
			{
				std::string element;
				getline(ss, element, '/');
				op.push_back(element);
			}
			return true;
		}
		return false;
	}

    //operation handlers
    //------------------
    void handleNapiError(nljson &jobj) {
        
        nljson::iterator jit;
        std::string opVal;
        napiError nErr {"ERROR.",{} };
        //error message specifies the operation
        if (hasKey(jobj, {"path"}, jit)) {
            opVal = jit.value();
            nErr.errorString += " Path: " + opVal;
        }
        
        //extract the array of errors
        if (hasKey(jobj, {"errors"}, jit)) {
            auto errorsList = jit.value();
            std::vector<std::pair<std::string, std::string> > errorsList1;
            nErr.errorString += ", Error message(s):";
            for (auto errPair : errorsList){
                std::string errType = errPair[1];
                std::string errMsg = errPair[0];
                
                nErr.errorList.push_back(std::make_pair(errMsg,errType));
                nErr.errorString += "{" + errType + ":" + "'" +errMsg + "'} ";
            }
        }
        
        //find the right callback to report this error on
        
        //if operation is a connected session operation...
        std::array<std::string,8> secureOp = {"random","symmetricKey","sign","totp","buzz","info","revoke"};
        if (std::any_of(secureOp.begin(),secureOp.end(), [&](std::string op){return opVal.find(op) != std::string::npos; })) {
            //we need an exchange to look up the callback
            std::string exchange;
            if (getExchange(jobj,exchange,false)){
                auto exchangeCallback = NymiProvision::nymiProvisions.find(exchange);
                if (exchangeCallback != NymiProvision::nymiProvisions.end()){
                    
                    //get pid
                    std::string pid = "";
                    getPid(jobj,pid);
                    
                    //report error on appropriate callback
                    if (opVal == "random/run") { exchangeCallback->second(failure,pid,"",nErr); }
                    else if (opVal == "symmetricKey/run") { exchangeCallback->second(failure,pid,nErr); }
                    else if (opVal == "symmetricKey/get") { exchangeCallback->second(failure,pid,"",nErr); }
                    else if (opVal == "totp/run") { exchangeCallback->second(failure,pid,nErr); }
                    else if (opVal == "totp/get") { exchangeCallback->second(failure,pid,"",nErr); }
                    else if (opVal == "sign/run") { exchangeCallback->second(failure,pid,"","",nErr); }
                    else if (opVal == "notify/run") { exchangeCallback->second(failure,pid, HapticNotification::NERROR,nErr); }
					else if (opVal == "revoke/run") { exchangeCallback->second(failure,pid,nErr); }
                    else if (opVal == "info") {
                        //since there is an exchangeCallback, this was a request to NymiProvision::getDeviceInfo()
                        
                        //get pid from the exchange
                        size_t pidstart = exchange.find("deviceinfo") + std::strlen("deviceinfo");
						pid = exchange.substr(pidstart);
                        
                        TransientNymiBandInfo blank;
                        exchangeCallback->second(failure,pid,blank,nErr);
                    }
                    
                    //remove <exchange,callback> entry
                    NymiProvision::nymiProvisions.erase(exchange);
                    return;
                }
            }
        }
        //report on general error callback
		std::cout << "Handing error to default onError callback" << std::endl;
        onError(nErr);
    }

    void handleOpProvision(nljson &jobj) {
        
		nljson::iterator jit;
		std::vector<std::string> op;
		if (getOperation(jobj, op)) {
			if (op[1] == "report") {
				if (op[2] == "patterns") {
					//handle receipt of provisioning pattern
					if (hasKey(jobj, { "event","patterns" }, jit)) {

						size_t num_patterns = jit.value().size();
						std::vector<std::string> patterns;

						for (unsigned int i = 0; i < num_patterns; ++i) {
							patterns.push_back(jit.value()[i]);
						}
						onAgreement(patterns);
					}
				}
				else if (op[2] == "provisioned") {
					//handle provisioned device
					if (hasKey(jobj, { "event","kind" }, jit) && jit.value() == "provisioned") {
						if (hasKey(jobj, { "event","info","pid" }, jit)) {
							std::string pid = jit.value();
							onProvision(NymiProvision(pid,""));
						}
					}
				}
			}
			else if (op[1] == "run" && (op[2] == "start" || op[2] == "stop")) {

				std::string provState = op[2];
				if (onProvisionModeChange) { onProvisionModeChange(provState); }
			}
		}
    }

	void handleOpProvisionsChanged(nljson &jobj) {
		std::vector<std::string> op;
		if (getOperation(jobj, op)) {
			if (op[1] == "changed") {
				// Got an updated list of known provisions from the API
				nljson::iterator jit;
				if (hasKey(jobj, { "response","provisions","provisions" }, jit)) {
					std::vector<NymiProvision> tempbands;
					for (auto prov : (nljson)jit.value()) {
						nljson::iterator tjit;
						std::string pid, provision;
						if (hasKey(prov, { "pid" }, tjit))
							pid = tjit.value();
						else
							pid = "";
						if (hasKey(prov, { "provision" }, tjit))
							provision = tjit.value();
						else
							provision = "";
						NymiProvision newprov(pid, provision);
						tempbands.push_back(newprov);
					}
					if (onProvisionChange)
						onProvisionChange(tempbands);
					else {
						std::string errMsg = "ERROR: onProvisionChange callback not set, ignoring.\n";
						napiError nErr = { errMsg,{} };
						onError(nErr);
						return;
					}
				}
				return;
			} else {
				std::string errMsg = "ERROR: Got a provisions message with unknown path field. Json response follows:\n";
				errMsg += jobj.dump();
				napiError nErr = { errMsg,{} };
				onError(nErr);
				return;
			}
		}
	}

    void handleOpInfo(nljson &jobj) {
        
        //we need an exchange to look up the callback
        std::string exchange;
        if (!getExchange(jobj,exchange,true)) return;
        
        nljson::iterator jit;
        
        if (exchange == "provisions" || exchange == "provisionsPresent") {
			std::vector<NymiProvision> provList;
            if (hasKey(jobj, {"response",exchange}, jit)) {
				auto napiProvList = jit.value();
				for (auto p : napiProvList) {
					std::string pid = p;
					provList.push_back(NymiProvision(pid,""));
				}
			}
            getProvisionList(provList);
        }
        else if (exchange.find("deviceinfo") != std::string::npos){
            
            //get pid from the exchange
            size_t pidstart = exchange.find("deviceinfo") + std::strlen("deviceinfo");
            std::string pid = exchange.substr(pidstart);
            if (hasKey(jobj,{"response","provisionMap"},jit)){
                std::map<std::string,int> provisionMap = jit.value();
                unsigned int idx = provisionMap[pid];
                if (hasKey(jobj,{"response","nymiband"},jit)){
                    auto nymiBands = jit.value();
                    if (idx < nymiBands.size()){
                        nljson deviceInfo = nymiBands[idx];
                        TransientNymiBandInfo ndinfo(deviceInfo);
                        
                        //send value to the callback associated with the exchange
                        auto exchangeCallback = NymiProvision::nymiProvisions.find(exchange);
                        if (exchangeCallback != NymiProvision::nymiProvisions.end()){
                            exchangeCallback->second(success,pid,ndinfo,noErr);
                            NymiProvision::nymiProvisions.erase(exchangeCallback);
                        }
                        else {
                            std::string errMsg = "ERROR. Received device info. No callback to NEA found. Json response follows:\n";
                            errMsg += jobj.dump();
                            napiError nErr = { errMsg, {} };
                            onError(nErr);
                            return;
                        }
                    }
                }
            }
        }
    }

    void handleOpRandom(nljson &jobj){
        
		// Check if this is the final response ("completed":true)
		nljson::iterator jit;
		if (!isKeyValue(jobj, { "completed" }, jit, true)) return;

        //we need an exchange to look up the callback
        std::string exchange;
        if (!getExchange(jobj,exchange,true)) return;

        //send value to the callback associated with the exchange
        auto exchangeCallback = NymiProvision::nymiProvisions.find(exchange);
        if (exchangeCallback != NymiProvision::nymiProvisions.end()){
            
            auto callbackFn = exchangeCallback->second;
            NymiProvision::nymiProvisions.erase(exchangeCallback);
            
            //get pid
            std::string pid;
            napiError nErr;
            if (!getPid(jobj,pid,nErr)) {
                callbackFn(failure,pid,"",nErr);   //pid is empty string
                return;
            }
            
            //get the value we want
            if (!hasKey(jobj, {"response","pseudoRandomNumber"}, jit)) {
                callbackFn(failure,pid,"",genMissingJsonKeyErr("pseudoRandomNumber",jobj));
                return;
            }
            std::string rand = jit.value();
        
            callbackFn(success,pid,rand,noErr);
            return;
        }
        else {
            std::string errMsg = "ERROR. Received pseudo random value. No callback to NEA found. Json response follows:\n";
            errMsg += jobj.dump();
            napiError nErr { errMsg, {} };
            onError(nErr);
            return;
        }
    }

    void handleOpSymmetric(nljson &jobj) {
		// Check if this is the final response ("completed":true)
		nljson::iterator jit;
		if (!isKeyValue(jobj, { "completed" }, jit, true)) return;

        //we need an exchange to look up the callback
        std::string exchange;
        if (!getExchange(jobj,exchange,true)) return;
        
        //send value to the callback associated with the exchange
        auto exchangeCallback = NymiProvision::nymiProvisions.find(exchange);
        if (exchangeCallback != NymiProvision::nymiProvisions.end()){
            
            auto callbackFn = exchangeCallback->second;
            NymiProvision::nymiProvisions.erase(exchangeCallback);
            
            //get pid
            std::string pid;
            napiError nErr;
            if (!getPid(jobj,pid)) {
                callbackFn(failure,pid,"",nErr);   //pid is empty string
                return;
            }
        
			std::vector<std::string> op;
            getOperation(jobj,op);
            if (op[1] == "run"){
                KeyType keyType = KeyType::SYMMETRIC;
                callbackFn(isKeyValue(jobj, {"successful"}, jit, true),pid,keyType,noErr);
            }
            else if (op[1] == "get"){
                if (hasKey(jobj,{"response","key"},jit)){
                    std::string key = jit.value();
                    callbackFn(success,pid,key,noErr);
                }
            }
            return;
        }
        else {
            std::string errMsg = "ERROR. Received symmetric key. No callback to NEA found. Json response follows:\n";
            errMsg += jobj.dump();
            napiError nErr { errMsg, {} };
            onError(nErr);
            return;
        }
    }

    void handleOpSignature(nljson&jobj) {
		// Check if this is the final response ("completed":true)
		nljson::iterator jit;
		if (!isKeyValue(jobj, { "completed" }, jit, true)) return;
        
        //we need an exchange to look up the callback
        std::string exchange;
        if (!getExchange(jobj,exchange,true)) return;
        
        //send value to the callback associated with the exchange
        auto exchangeCallback = NymiProvision::nymiProvisions.find(exchange);
        if (exchangeCallback != NymiProvision::nymiProvisions.end()){
            
            auto callbackFn = exchangeCallback->second;
            NymiProvision::nymiProvisions.erase(exchangeCallback);
            
            //get pid
            std::string pid;
            napiError nErr;
            if (!getPid(jobj,pid,nErr)) {
                callbackFn(failure,pid,"","",nErr);   //pid is empty string
                return;
            }

			// Is this setup or a signing response?
			std::vector<std::string> op;
			getOperation(jobj, op);
			if (op[1] == "setup") {
				if (isKeyValue(jobj, { "successful" }, jit, true)) {
					callbackFn(success, pid, noErr);
				} else {
					callbackFn(failure, pid, nErr);
				}
			}
			else if (op[1] == "run") {
				//get the value we want
				if (!hasKey(jobj, { "response","signature" }, jit)) {

					callbackFn(failure, pid, "", "", genMissingJsonKeyErr("signature", jobj));
					return;
				}
				std::string sig = jit.value();

				if (!hasKey(jobj, { "response","verificationKey" }, jit)) {

					callbackFn(failure, pid, "", "", genMissingJsonKeyErr("verificationKey", jobj));
					return;
				}
				std::string vk = jit.value();

				//send value to the callback associated with the exchange
				callbackFn(success, pid, sig, vk, noErr);
				return;
			}
        }
        else {
            std::string errMsg = "ERROR. Received signature. No callback to NEA found. Json response follows:\n";
            errMsg += jobj.dump();
            napiError nErr { errMsg, {} };
            onError(nErr);
            return;
        }
    }

    void handleOpTotp(nljson&jobj) {
		// Check if this is the final response ("completed":true)
		nljson::iterator jit;
		if (!isKeyValue(jobj, { "completed" }, jit, true)) return;

        //we need an exchange to look up the callback
        std::string exchange;
        if (!getExchange(jobj,exchange,true)) return;
        
        //send value to the callback associated with the exchange
        auto exchangeCallback = NymiProvision::nymiProvisions.find(exchange);
        if (exchangeCallback != NymiProvision::nymiProvisions.end()){
        
            auto callbackFn = exchangeCallback->second;
            NymiProvision::nymiProvisions.erase(exchangeCallback);
            
            //get pid
            std::string pid;
            napiError nErr;
            if (!getPid(jobj,pid)) {
                callbackFn(failure,pid,nErr);   //pid is empty string
                return;
            }
            
            //get the value we want
            if (!isKeyValue(jobj, {"successful"}, jit, true)) {
                std::string errMsg = "Could not complete CreateTOTP request. JSON response follows:\n";
                errMsg += jobj.dump();
                napiError nErr { errMsg, {} };
                callbackFn(failure,pid,nErr);
                return;
            }
            
			std::vector<std::string> op;
			getOperation(jobj, op);
            if (op[1] == "run"){
                KeyType keyType = KeyType::TOTP;
                callbackFn(isKeyValue(jobj, {"successful"}, jit, true),pid,keyType,noErr);
            }
            else if (op[1] == "get"){
                if (!hasKey(jobj, {"response","totp"}, jit)) {
                    callbackFn(failure,pid,"",genMissingJsonKeyErr("response/totp",jobj));
                    return;
                }
                std::string totpid = jit.value();
                callbackFn(success,pid,totpid,noErr);
            }
            
            return;
        }
        else {
            std::string errMsg = "ERROR in Totp operation. No callback to NEA found. Json response follows:\n";
            errMsg += jobj.dump();
            napiError nErr { errMsg, {} };
            onError(nErr);
            return;
        }
    }

    void handleOpNotified(nljson&jobj) {
		// Check if this is the final response ("completed":true)
		nljson::iterator jit;
		if (!isKeyValue(jobj, { "completed" }, jit, true)) return;

        //we need an exchange to look up the callback
        std::string exchange;
        if (!getExchange(jobj,exchange,true)) return;
        
        //send value to the callback associated with the exchange
        auto exchangeCallback = NymiProvision::nymiProvisions.find(exchange);
        if (exchangeCallback != NymiProvision::nymiProvisions.end()){
            
            auto callbackFn = exchangeCallback->second;
            NymiProvision::nymiProvisions.erase(exchangeCallback);
            
            //get pid
            std::string pid;
            napiError nErr;
            if (!getPid(jobj,pid,nErr)) {
                callbackFn(failure,pid,"",nErr);   //pid is empty string
                return;
            }
        
            if (!hasKey(jobj, {"request","buzz"}, jit)) {

                callbackFn(failure,pid,HapticNotification::NERROR,genMissingJsonKeyErr("request/buzz",jobj));
                return;
            }
            
            bool notifyVal= jit.value();
            HapticNotification notifyType = (notifyVal) ? HapticNotification::NOTIFY_POSITIVE : HapticNotification::NOTIFY_NEGATIVE;
            
            //send value to the callback associated with the exchange
            callbackFn(success,pid,notifyType,noErr);
            return;
        }
        else {
            std::string errMsg = "ERROR. Received Notification result. No callback to NEA found. Json response follows:\n";
            errMsg += jobj.dump();
            napiError nErr { errMsg, {} };
            onError(nErr);
            return;
        }
    }

    void handleOpApiNotifications(nljson &jobj) {
        
		std::vector<std::string> op;
        nljson::iterator jit;
        getOperation(jobj, op);
        
        if (op[1] == "set"){/*response of set is received here, not handling it for now*/}
        else if (op[1] == "report") {
            
            if (hasKey(jobj, {"event","kind"},jit)){
                
                std::string eventType = jit.value();

                if (eventType == "found-change" || eventType == "presence-change"){
                    
                    std::string before, after, pid;
                    
                    if(hasKey(jobj, {"event","before"},jit)) { before = jit.value(); }
                    if(hasKey(jobj, {"event","after"},jit)) { after = jit.value(); }
                    if (hasKey(jobj, {"event","pid"}, jit)) { pid = jit.value(); }
                    
                    if (eventType == "found-change"){
                        onFoundChange(pid,stringToFoundStatus(before),stringToFoundStatus(after));
                    }
                    else if (eventType == "presence-change"){
                        
                        bool authenticated = false;
                        if (hasKey(jobj, {"event","authenticated"},jit)){ authenticated = jit.value(); }
                        onPresenceChange(pid,stringToPresenceStatus(before),stringToPresenceStatus(after),authenticated);
                    }
                }
            }
        }
        else if (op[1] == "get" && hasKey(jobj,{"response"},jit)) {
            
            std::map<std::string,bool> notificationsState = jit.value();
            onNotificationsGet(notificationsState);
        }
    }
    
    void handleOpRevokeProvision(nljson &jobj) {
		// Check if this is the final response ("completed":true)
		nljson::iterator jit;
		if (!isKeyValue(jobj, { "completed" }, jit, true)) return;

        //we need an exchange to look up the callback
        std::string exchange;
        if (!getExchange(jobj,exchange,true)) return;

        //send value to the callback associated with the exchange
        auto exchangeCallback = NymiProvision::nymiProvisions.find(exchange);
        if (exchangeCallback != NymiProvision::nymiProvisions.end()){

            auto callbackFn = exchangeCallback->second;
            NymiProvision::nymiProvisions.erase(exchangeCallback);

            //get pid
            std::string pid;
            napiError nErr;
            if (!getPid(jobj,pid,nErr)) {
                callbackFn(failure,pid,nErr);   //pid is empty string
                return;
            }

            //send value to the callback associated with the exchange
            callbackFn(success,pid,noErr);
            return;
        }
        else {
            std::string errMsg = "ERROR. Revoked provision successfully. No callback to NEA found. Json response follows:\n";
            errMsg += jobj.dump();
            napiError nErr { errMsg, {} };
            onError(nErr);
            return;
        }
    }

    void handleOpKey(nljson &jobj){
		// Check if this is the final response ("completed":true)
		nljson::iterator jit;
		if (!isKeyValue(jobj, { "completed" }, jit, true)) return;

        //we need an exchange to look up the callback
        std::string exchange;
        if (!getExchange(jobj,exchange,true)) return;

        //send value to the callback associated with the exchange
        auto exchangeCallback = NymiProvision::nymiProvisions.find(exchange);
        if (exchangeCallback != NymiProvision::nymiProvisions.end()){

            auto callbackFn = exchangeCallback->second;
            NymiProvision::nymiProvisions.erase(exchangeCallback);

            //get pid
            std::string pid;
            napiError nErr;
            if (!getPid(jobj,pid,nErr)) {
                KeyType keyType = KeyType::NERROR;
                callbackFn(failure,pid,keyType,nErr);   //pid is empty string
                return;
            }

            //send value to the callback associated with the exchange
            KeyType keyType = KeyType::NERROR;
            if (isKeyValue(jobj, {"request","symmetric"},jit,true) && isKeyValue(jobj, {"response","symmetric"},jit,false)){
                keyType = KeyType::SYMMETRIC;
            }
            else if (isKeyValue(jobj, {"request","totp"},jit,true) && isKeyValue(jobj, {"response","totp"},jit,false)){
                keyType = KeyType::TOTP;
            }
            callbackFn(success,pid,keyType,noErr);
            return;
        }
        else {
            std::string errMsg = "ERROR. Revoked provision successfully. No callback to NEA found. Json response follows:\n";
            errMsg += jobj.dump();
            napiError nErr { errMsg, {} };
            onError(nErr);
            return;
        }
    }

}   //end namespace Listener
