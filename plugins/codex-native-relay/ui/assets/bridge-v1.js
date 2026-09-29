// Sub2API UI Bridge v1 客户端：校验来源窗口、消息标识、Bridge Token 与 request_id。
(function () {
  "use strict";
  var UI_SOURCE = "sub2api-plugin-ui";
  var HOST_SOURCE = "sub2api-plugin-host";
  var TIMEOUT_MS = 30000;

  function readToken() {
    var match = /(?:^|[#&])bridge_token=([^&]+)/.exec(window.location.hash || "");
    return match ? decodeURIComponent(match[1]) : "";
  }

  function createBridge() {
    var token = readToken();
    var pending = new Map();
    var counter = 0;

    function onMessage(event) {
      if (event.source !== window.parent) return;
      var data = event.data;
      if (!data || data.source !== HOST_SOURCE || data.bridge_token !== token) return;
      var entry = pending.get(data.request_id);
      if (!entry || data.type !== entry.type + ".result") return;
      pending.delete(data.request_id);
      clearTimeout(entry.timer);
      if (data.ok === false && data.error) entry.reject(new Error(String(data.error)));
      else entry.resolve(data);
    }

    function post(type, extra) {
      var message = Object.assign({ source: UI_SOURCE, bridge_token: token, type: type }, extra || {});
      window.parent.postMessage(message, "*");
    }

    function request(type, extra) {
      counter += 1;
      var requestID = type + "-" + Date.now().toString(36) + "-" + counter;
      return new Promise(function (resolve, reject) {
        var timer = setTimeout(function () {
          pending.delete(requestID);
          reject(new Error("请求超时"));
        }, TIMEOUT_MS);
        pending.set(requestID, { type: type, resolve: resolve, reject: reject, timer: timer });
        post(type, Object.assign({ request_id: requestID }, extra || {}));
      });
    }

    window.addEventListener("message", onMessage);

    return {
      ready: function () { post("sub2api.plugin.ready"); },
      loadConfig: function () { return request("config.load"); },
      saveConfig: function (config) { return request("config.save", { config: config }); },
      testConfig: function () { return request("config.test"); },
      status: function () { return request("plugin.status"); },
      resize: function (height) { post("ui.resize", { height: height }); },
      notify: function (level, message) { post("ui.notify", { level: level, message: message }); },
      dispose: function () {
        window.removeEventListener("message", onMessage);
        pending.forEach(function (entry) {
          clearTimeout(entry.timer);
          entry.reject(new Error("页面已卸载"));
        });
        pending.clear();
      }
    };
  }

  window.Sub2APIBridge = { create: createBridge };
})();
