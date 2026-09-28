// Google Analytics with consent, for CORDIS Analytics.
// Injected once per page load into the Streamlit page (the parent of the
// components.html iframe) by app.py, which replaces __GA_ID__ with the Measurement ID.
//
// - Nothing is loaded from Google until the visitor clicks Accept.
// - The choice is kept in localStorage (cordis_ga_consent = granted | denied).
// - Consent Mode v2: everything denied by default; only analytics_storage is
//   granted after Accept. Ad storage/user data/personalisation stay denied.
// - page_location drops the query string (except utm_*) and the hash.
// - Automated browsers (navigator.webdriver, headless or bot user agents) never load GA.
(function () {
  var GA_ID = __GA_ID__;
  var KEY = "cordis_ga_consent";
  var w = window, d = document;

  if (w.__cordisConsentInit) return;  // once per page load, whatever Streamlit reruns
  w.__cordisConsentInit = true;

  function getChoice() {
    try { return w.localStorage.getItem(KEY); } catch (e) { return null; }
  }
  function setChoice(value) {
    try { w.localStorage.setItem(KEY, value); } catch (e) { /* private mode: ask again next time */ }
  }
  function clearChoice() {
    try { w.localStorage.removeItem(KEY); } catch (e) { /* ignore */ }
  }

  function isBot() {
    var ua = (w.navigator && w.navigator.userAgent) || "";
    return w.navigator.webdriver === true || /HeadlessChrome|Headless|bot/i.test(ua);
  }

  // Origin + path, keeping only utm_* query parameters; no hash. On Streamlit Cloud
  // the app runs under /~/+/, which is removed so pages report as "/".
  function cleanLocation() {
    var u = new URL(w.location.href);
    var keep = new URLSearchParams();
    u.searchParams.forEach(function (value, key) {
      if (/^utm_/i.test(key)) keep.append(key, value);
    });
    var path = u.pathname.replace(/^\/~\/\+/, "") || "/";
    var query = keep.toString();
    return u.origin + path + (query ? "?" + query : "");
  }

  function loadGA() {
    if (isBot()) return;
    w["ga-disable-" + GA_ID] = false;
    if (d.getElementById("ga-script-tag")) {  // re-accepted after withdrawing on this page
      w.gtag("consent", "update", { analytics_storage: "granted" });
      return;
    }
    w.dataLayer = w.dataLayer || [];
    w.gtag = function () { w.dataLayer.push(arguments); };
    w.gtag("consent", "default", {
      analytics_storage: "denied",
      ad_storage: "denied",
      ad_user_data: "denied",
      ad_personalization: "denied"
    });
    w.gtag("consent", "update", { analytics_storage: "granted" });
    w.gtag("js", new Date());
    w.gtag("config", GA_ID, { page_location: cleanLocation() });
    var s = d.createElement("script");
    s.id = "ga-script-tag";
    s.async = true;
    s.src = "https://www.googletagmanager.com/gtag/js?id=" + encodeURIComponent(GA_ID);
    d.head.appendChild(s);
  }

  function deleteGaCookies() {
    var parts = w.location.hostname.split(".");
    var domains = ["", w.location.hostname];
    for (var i = 0; i < parts.length - 1; i++) domains.push("." + parts.slice(i).join("."));
    d.cookie.split(";").forEach(function (c) {
      var name = c.split("=")[0].trim();
      if (name.indexOf("_ga") !== 0) return;
      domains.forEach(function (dom) {
        d.cookie = name + "=; expires=Thu, 01 Jan 1970 00:00:00 GMT; path=/" +
                   (dom ? "; domain=" + dom : "");
      });
    });
  }

  function withdraw() {
    clearChoice();
    w["ga-disable-" + GA_ID] = true;
    if (w.gtag) w.gtag("consent", "update", { analytics_storage: "denied" });
    deleteGaCookies();
  }

  function button(label, onClick) {
    var b = d.createElement("button");
    b.type = "button";
    b.textContent = label;
    // Accept and Decline share this exact style: equally prominent
    b.style.cssText = "background:#003399;color:#fff;border:0;border-radius:6px;" +
                      "padding:6px 16px;font:inherit;font-weight:600;cursor:pointer;";
    b.addEventListener("click", onClick);
    return b;
  }

  function hideBanner() {
    var b = d.getElementById("cordis-consent-banner");
    if (b) b.remove();
  }

  function choose(value) {
    setChoice(value);
    hideBanner();
    if (value === "granted") loadGA();
    else deleteGaCookies();  // e.g. cookies left over from before consent was asked
  }

  function showBanner() {
    if (d.getElementById("cordis-consent-banner")) return;
    var bar = d.createElement("div");
    bar.id = "cordis-consent-banner";
    bar.setAttribute("role", "dialog");
    bar.setAttribute("aria-label", "Cookie consent");
    bar.style.cssText = "position:fixed;left:50%;transform:translateX(-50%);bottom:16px;" +
      "z-index:999999;background:#fff;color:#1f2937;border:1px solid #d1d5db;" +
      "border-radius:8px;box-shadow:0 4px 16px rgba(0,0,0,.15);padding:10px 14px;" +
      "display:flex;align-items:center;gap:10px;flex-wrap:wrap;" +
      "max-width:calc(100% - 32px);font:14px/1.4 'Source Sans Pro',sans-serif;";
    var text = d.createElement("span");
    text.textContent = "This site uses Google Analytics to count visits.";
    var close = d.createElement("button");
    close.type = "button";
    close.textContent = "×";
    close.setAttribute("aria-label", "Close (decline)");
    close.style.cssText = "background:none;border:0;font-size:18px;line-height:1;" +
                          "color:#6b7280;cursor:pointer;padding:0 4px;";
    close.addEventListener("click", function () { choose("denied"); });  // close = Decline
    bar.appendChild(text);
    bar.appendChild(button("Accept", function () { choose("granted"); }));
    bar.appendChild(button("Decline", function () { choose("denied"); }));
    bar.appendChild(close);
    d.body.appendChild(bar);
  }

  function addSettingsLink() {
    if (d.getElementById("cordis-cookie-settings")) return;
    var a = d.createElement("a");
    a.id = "cordis-cookie-settings";
    a.href = "#";
    a.textContent = "Cookie settings";
    a.style.cssText = "position:fixed;left:12px;bottom:8px;z-index:999998;" +
      "font:12px 'Source Sans Pro',sans-serif;color:#6b7280;text-decoration:underline;" +
      "background:rgba(255,255,255,.85);padding:2px 6px;border-radius:4px;cursor:pointer;";
    a.addEventListener("click", function (e) {
      e.preventDefault();
      withdraw();
      showBanner();
    });
    d.body.appendChild(a);
  }

  var choice = getChoice();
  if (choice === "granted") loadGA();
  else if (choice !== "denied") showBanner();
  addSettingsLink();
})();
