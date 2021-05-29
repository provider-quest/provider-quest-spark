const { load } = require("@alex.garcia/observable-prerender");
(async () => {
  const notebook = await load("@d3/bar-chart", ["chart", "data"]);
  const data = [
    { name: "alex", value: 20 },
    { name: "brian", value: 30 },
    { name: "craig", value: 10 },
  ];
  await notebook.redefine("data", data);
  await notebook.screenshot("chart", "bar-chart.png");
  await notebook.browser.close();
})();
