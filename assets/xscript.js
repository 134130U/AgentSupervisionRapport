
$(window).on("load", function() {
  var myChart = new DonutChart(document.getElementById("donutchart"), {
    r: 60,
    stroke: 16,
    scale: 100,
    items: [
      { label: "A", value: .2 },
      { label: "B", value: .1 },
      { label: "C", value: .5 },
    ]
})
})
