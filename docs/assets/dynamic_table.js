/**
 * dynamic_table.js
 * Fetches a CSV file and renders it as a searchable, filterable DataTable.
 */

function parseCSV(text) {
  const lines = text.trim().split("\n");
  const headers = lines[0].split(",");
  const data = lines.slice(1).map(line => {
    const values = line.split(",");
    const obj = {};
    headers.forEach((h, i) => (obj[h.trim()] = values[i] ? values[i].trim() : ""));
    return obj;
  });
  return { headers, data };
}

function loadCSVTable(csvPath) {
  fetch(csvPath)
    .then(res => {
      if (!res.ok) throw new Error(`Failed to fetch ${csvPath}`);
      return res.text();
    })
    .then(text => {
      const { headers, data } = parseCSV(text);

      // Convert objects to array of arrays
      const rows = data.map(d => headers.map(h => d[h]));

      // Render DataTable
      $("#csv-table").DataTable({
        data: rows,
        columns: headers.map(h => ({ title: h })),
        pageLength: 10,
        searching: true,
        scrollX: true,
        responsive: true
      });
    })
    .catch(err => {
      console.error("Error loading CSV:", err);
      document.getElementById("table-container").innerHTML =
        "<p>⚠️ Failed to load data table.</p>";
    });
}

// Automatically initialize
document.addEventListener("DOMContentLoaded", () => {
  loadCSVTable("/data/produits.csv");
});
