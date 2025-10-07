# ✅ Markeer een onboarding-stap als voltooid
# step: bijvoorbeeld 1 = setup, 2 = technisch, 3 = macro, 4 = dashboard

export async function markStepDone(step) {
  const userId = localStorage.getItem("user_id");
  if (!userId) {
    console.warn("⚠️ Geen user_id gevonden in localStorage");
    return;
  }

  const stepMapping = {
    1: "setup_done",
    2: "technical_done",
    3: "macro_done",
    4: "dashboard_done"
  };
  const stepKey = stepMapping[step];

  if (!stepKey) {
    console.warn("⚠️ Ongeldige stapnummer:", step);
    return;
  }

  const payload = { step: stepKey, done: true };

  try {
    const res = await fetch(`/onboarding_status/${userId}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });

    if (!res.ok) throw new Error(`Serverfout: ${res.status}`);

    console.log(`✅ Stap ${step} (${stepKey}) voltooid.`);

    // ✅ UI bijwerken
    const statusEl = document.getElementById(`step${step}-status`);
    if (statusEl) {
      statusEl.classList.remove("todo");
      statusEl.classList.add("done");
      statusEl.textContent = "✅";
    }

    // ✅ Voortgangsbalk berekenen
    const allSteps = document.querySelectorAll(".onboarding-steps li");
    const doneSteps = Array.from(allSteps).filter(li => li.querySelector(".done")).length;
    const progress = Math.round((doneSteps / allSteps.length) * 100);
    const bar = document.getElementById("progress");
    if (bar) bar.style.width = `${progress}%`;

    // ✅ Toon afrondingsbanner
    if (doneSteps === allSteps.length) {
      const doneBox = document.getElementById("onboarding-done");
      if (doneBox) doneBox.style.display = "block";
    }

  } catch (err) {
    console.error(`❌ Fout bij updaten onboarding stap ${step}:`, err);
  }
}
