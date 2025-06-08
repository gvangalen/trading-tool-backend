// ✅ onboarding_helper.js

/**
 * ✅ Markeer een onboarding-stap als voltooid
 * @param {number} step - Bijvoorbeeld 1 voor setup, 2 voor technisch, enz.
 */
export async function markStepDone(step) {
  const userId = localStorage.getItem("user_id");
  if (!userId) {
    console.warn("⚠️ Geen user_id gevonden in localStorage");
    return;
  }

  // Stap vertalen naar backend field
  const stepMapping = {
    1: "setup_done",
    2: "technical_done",
    3: "macro_done",
    4: "dashboard_done"
  };
  const stepKey = stepMapping[step];
  if (!stepKey) {
    console.warn("⚠️ Ongeldige stapnummer");
    return;
  }

  const payload = { step: stepKey, done: true };

  try {
    const res = await fetch(`/onboarding_status/${userId}`, {
      method: "PUT", // ✅ Gebruik PUT i.p.v. PATCH
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });

    if (!res.ok) throw new Error("Update mislukt");
    console.log(`✅ Onboarding stap ${step} (${stepKey}) gemarkeerd als voltooid.`);

    // ✅ Lokale status bijwerken
    const statusEl = document.getElementById(`step${step}-status`);
    if (statusEl) {
      statusEl.classList.remove("todo");
      statusEl.classList.add("done");
      statusEl.textContent = "✅";
    }

    // ✅ Voortgangsbalk opnieuw berekenen
    const allSteps = document.querySelectorAll(".onboarding-steps li");
    const doneSteps = document.querySelectorAll(".onboarding-steps li .done").length;
    const progress = Math.round((doneSteps / allSteps.length) * 100);
    const bar = document.getElementById("progress");
    if (bar) bar.style.width = `${progress}%`;

    // ✅ Alles voltooid? Laat banner zien
    if (doneSteps === allSteps.length) {
      const doneBox = document.getElementById("onboarding-done");
      if (doneBox) doneBox.style.display = "block";
    }

  } catch (err) {
    console.error(`❌ Fout bij onboarding update stap ${step}:`, err);
  }
}
