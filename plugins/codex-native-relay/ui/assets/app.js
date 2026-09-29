(function () {
  "use strict";
  var bridge = window.Sub2APIBridge.create();
  var form = document.getElementById("config-form");
  var saveButton = document.getElementById("save");
  var testButton = document.getElementById("test");
  var dirtyHint = document.getElementById("dirty");
  var result = document.getElementById("result");
  var textFields = ["relay_url", "relay_secret", "credential_mode", "unmatched_requests"];
  var numberFields = ["response_header_timeout_seconds", "direct_response_header_timeout_seconds"];

  function field(id) { return document.getElementById(id); }

  var defaults = {
    relay_url: "http://127.0.0.1:8788",
    credential_mode: "codex",
    unmatched_requests: "direct",
    response_header_timeout_seconds: 300,
    direct_response_header_timeout_seconds: 120
  };

  function render(config) {
    config = Object.assign({}, defaults, config || {});
    textFields.forEach(function (id) { field(id).value = config[id] != null ? String(config[id]) : ""; });
    numberFields.forEach(function (id) { field(id).value = config[id] != null ? String(config[id]) : ""; });
    field("allow_remote_relay").checked = Boolean(config.allow_remote_relay);
    setDirty(false);
  }

  function collect() {
    var config = {};
    textFields.forEach(function (id) { config[id] = field(id).value.trim(); });
    numberFields.forEach(function (id) {
      var value = field(id).value.trim();
      if (value !== "") config[id] = Number(value);
    });
    config.allow_remote_relay = field("allow_remote_relay").checked;
    return config;
  }

  function setDirty(dirty) { dirtyHint.hidden = !dirty; }

  function setBusy(busy) {
    saveButton.disabled = busy;
    testButton.disabled = busy;
  }

  function show(ok, message) {
    result.hidden = false;
    result.className = "result " + (ok ? "ok" : "err");
    result.textContent = message;
    resize();
  }

  function resize() { bridge.resize(document.documentElement.scrollHeight + 16); }

  function save() {
    return bridge.saveConfig(collect()).then(function (data) {
      render(data.config);
      return data;
    });
  }

  form.addEventListener("input", function () { setDirty(true); });

  form.addEventListener("submit", function (event) {
    event.preventDefault();
    setBusy(true);
    save()
      .then(function () { show(true, "配置已保存"); })
      .catch(function (error) { show(false, error.message); })
      .finally(function () { setBusy(false); });
  });

  testButton.addEventListener("click", function () {
    setBusy(true);
    save()
      .then(function () { return bridge.testConfig(); })
      .then(function (data) {
        var outcome = data.result || {};
        var latency = outcome.latency_ms != null ? "（" + outcome.latency_ms + " ms）" : "";
        show(Boolean(outcome.success), (outcome.message || "") + latency);
      })
      .catch(function (error) { show(false, error.message); })
      .finally(function () { setBusy(false); });
  });

  window.addEventListener("unload", function () { bridge.dispose(); });

  bridge.ready();
  bridge.loadConfig()
    .then(function (data) { render(data.config); resize(); })
    .catch(function (error) { show(false, "读取配置失败：" + error.message); });
})();
