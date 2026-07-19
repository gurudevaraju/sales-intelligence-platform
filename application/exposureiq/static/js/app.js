// Auto-submit filter forms when a checkbox/select changes
document.addEventListener("change", (e) => {
  if (e.target.closest("form.filters-form") && e.target.matches("select, input[type=checkbox]")) {
    e.target.closest("form").requestSubmit();
  }
});

// Range sliders: live-update the visible number, then debounce-submit
document.querySelectorAll(".filters-form input[type=range]").forEach((input) => {
  const label = input.parentElement.querySelector(".f-range-label span");
  const sync = () => { if (label) label.textContent = input.value; };
  sync();
  let t;
  input.addEventListener("input", () => {
    sync();
    clearTimeout(t);
    t = setTimeout(() => input.closest("form")?.requestSubmit(), 400);
  });
});

// Debounced text/number search submit
document.querySelectorAll(".search-box input, .f-group input[type=text], .f-group input[type=number]").forEach((input) => {
  let t;
  input.addEventListener("input", () => {
    clearTimeout(t);
    t = setTimeout(() => input.closest("form")?.requestSubmit(), 450);
  });
});

// Page size selector auto-submits
document.addEventListener("change", (e) => {
  if (e.target.matches(".pg-size select")) {
    e.target.closest("form").requestSubmit();
  }
});

// Lead status dropdown -> POST update, no full reload
document.addEventListener("change", async (e) => {
  if (e.target.matches(".status-select")) {
    const company = e.target.dataset.company;
    const status = e.target.value;
    e.target.className = "status-select status-" + status.replace(/\s+/g, "");
    try {
      await fetch(`/lead/${encodeURIComponent(company)}/status`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status }),
      });
    } catch (err) {
      console.error("Failed to update status", err);
    }
  }
});

// Notes save (company detail page)
async function saveNotes(company) {
  const notes = document.getElementById("notes-field").value;
  const status = document.getElementById("status-field").value;
  const btn = document.getElementById("save-notes-btn");
  btn.textContent = "Saving…";
  await fetch(`/lead/${encodeURIComponent(company)}/status`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ status, notes }),
  });
  btn.textContent = "Saved ✓";
  setTimeout(() => (btn.textContent = "Save"), 1200);
}

// Simple tabs
function showTab(name, groupId) {
  const group = document.getElementById(groupId);
  group.querySelectorAll(".tab").forEach((t) => t.classList.toggle("active", t.dataset.tab === name));
  group.querySelectorAll(".tab-panel").forEach((p) => p.classList.toggle("active", p.dataset.panel === name));
}

// ---- Reports: chart builder dataset toggle ----
function onDatasetChange(sel) {
  const val = sel.value;
  document.querySelectorAll(".dataset-fields").forEach((el) => {
    el.classList.toggle("active", el.dataset.dataset === val);
  });
}

// ---- Reports: delete a saved chart ----
async function deleteChart(id) {
  if (!confirm("Remove this saved graph?")) return;
  await fetch(`/reports/charts/${id}/delete`, { method: "POST" });
  document.getElementById("chart-" + id)?.remove();
}
