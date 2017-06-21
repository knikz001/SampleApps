#pragma once

#include <thread>
#include "ProvisionStorage.h"
#include "NeaCallbackTypes.h"
#include "NymiProvision.h"
#include "napi.h"

#ifdef _WIN32
#include <windows.h>
// Only used by Windows
// Setup types for DLL runtime loading
typedef napi::ConfigOutcome (WINAPI *PCONFIG)(const char*, const char*, const char*, napi::LogLevel, int, const char*);
typedef napi::PutOutcome (WINAPI *PPUT)(const char*);
typedef napi::GetOutcome (WINAPI *PGET)(char*, unsigned long long, unsigned long long*);
typedef napi::TryGetOutcome(WINAPI *PTRYGET)(char*, unsigned long long, unsigned long long *);
typedef void (WINAPI *PTERMINATE)();
#endif // _WIN32

class NymiApi {

public:

	~NymiApi();

	enum class ProvisionListType { ALL, PRESENT };

    static NymiApi *getNymiApi(napi::ConfigOutcome &initResult, errorCallback onError, std::string rootDirectory, napi::LogLevel log = napi::LogLevel::normal, int nymulatorPort = -1, std::string nymulatorHost = "");
	
	bool startProvisioning(agreementCallback onPattern, newProvisionCallback onProvision);
	void acceptPattern(std::string pattern);
	void stopProvisioning();
	bool getProvisions(getProvisionsCallback getProvList, ProvisionListType type);
	bool setOnProvisionsChange(provisionChangeCallback onProvisionsChange);
    bool setOnProvisionModeChange(onStartStopProvisioning onProvisionModeChange);

	bool setOnFoundChange(onNymiBandFoundStatusChange onFoundChange);
    bool setOnPresenceChange(onNymiBandPresenceChange onPresenceChange);
    void disableOnFoundChange();
    void disableOnPresenceChange();
    bool getApiNotificationState(onNotificationsGetState onNotificationsGet);
	bool save(std::vector<NymiProvision> &curbands);

#ifdef _WIN32
// Variables for handling dynamic loading of DLL
private:
	// NAPI DLL Handle
	HINSTANCE m_hNapi;
public:
	// Pointers for dynamically loading exported NAPI calls
	PCONFIG configure;
	PPUT put;
	PTERMINATE terminate;
	PGET get;
	PTRYGET try_get;
#endif
private:
	ProvisionStorage *storage;  // Provisioning file handler object
	// Initialization and singleton pattern
	static NymiApi *nApi;
    NymiApi() : storage(nullptr) { /*intentionally empty*/ };
	NymiApi(NymiApi &dontAllowCopy) { /*intentionally empty*/ }
    NymiApi(NymiApi &&dontAllowMove) { /*intentionally empty*/ }

	void init(napi::ConfigOutcome &initResult, std::string rootDirectory, napi::LogLevel log, int nymulatorPort = -1, std::string nymulatorHost = "");


	// Receive json communication from napi
	std::thread listener;
public:
	// Portable put (just saves some extra defines for Windows vs. Other)
	napi::PutOutcome portable_put(std::string json);
};
