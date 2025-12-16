"use client";

import { useState, useEffect, useCallback, useMemo } from "react";

import {
  getOnboardingStatus,
  completeOnboardingStep,
  finishOnboarding,
  resetOnboarding,
} from "@/lib/api/onboarding";

export function useOnboarding() {
  const [status, setStatus] = useState(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState(null);

  // =====================================================
  // 1️ Status ophalen
  // =====================================================
  const fetchStatus = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);

      console.log("[Onboarding] Fetch status...");
      const data = await getOnboardingStatus();
      console.log("[Onboarding] Status ontvangen:", data);

      setStatus(data);
    } catch (err) {
      console.error("[Onboarding] ❌ Failed to load onboarding status:", err);
      setError("Kon onboarding-status niet laden.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchStatus();
  }, [fetchStatus]);

  // =====================================================
  // 2️ Acties
  // =====================================================
  const completeStep = async (step, metaReason = "unknown") => {
    try {
      setSaving(true);
      setError(null);

      console.log(`[Onboarding] completeStep('${step}') reason=${metaReason}`);
      const res = await completeOnboardingStep(step);
      console.log(`[Onboarding] ✅ completeStep('${step}') response:`, res);

      await fetchStatus();
    } catch (err) {
      console.error(`[Onboarding] ❌ Complete step failed (${step}):`, err);
      setError("Stap kon niet worden voltooid.");
    } finally {
      setSaving(false);
    }
  };

  const finish = async () => {
    try {
      setSaving(true);
      setError(null);

      console.log("[Onboarding] finish()");
      await finishOnboarding();
      await fetchStatus();
    } catch (err) {
      console.error("[Onboarding] ❌ Finish onboarding failed:", err);
      setError("Onboarding afronden mislukt.");
    } finally {
      setSaving(false);
    }
  };

  const reset = async () => {
    try {
      setSaving(true);
      setError(null);

      console.log("[Onboarding] reset()");
      await resetOnboarding();
      await fetchStatus();
    } catch (err) {
      console.error("[Onboarding] ❌ Reset onboarding failed:", err);
      setError("Onboarding reset mislukt.");
    } finally {
      setSaving(false);
    }
  };

  // =====================================================
  // 3️ Stap-status
  // =====================================================
  const stepStatus = useMemo(() => {
    if (!status) return null;

    const s = {
      market: !!status.has_market,
      macro: !!status.has_macro,
      technical: !!status.has_technical,
      setup: !!status.has_setup,
      strategy: !!status.has_strategy,
    };

    console.log("[Onboarding] stepStatus:", s);
    return s;
  }, [status]);

  // =====================================================
  // 4️ Onboarding & pipeline status
  // =====================================================
  const onboardingComplete = useMemo(() => {
    if (!stepStatus) return false;
    const ok = Object.values(stepStatus).every(Boolean);
    console.log("[Onboarding] onboardingComplete =", ok);
    return ok;
  }, [stepStatus]);

  const pipelineStarted = !!status?.pipeline_started;

  const pipelineRunning = onboardingComplete && !pipelineStarted;
  const dashboardReady = onboardingComplete && pipelineStarted;

  // =====================================================
  // 5️ Unlock logic (volgorde)
  // =====================================================
  const allowedSteps = useMemo(() => {
    const a = {
      market: true,
      macro: stepStatus?.market ?? false,
      technical: stepStatus?.macro ?? false,
      setup: stepStatus?.technical ?? false,
      strategy: stepStatus?.setup ?? false,
    };

    console.log("[Onboarding] allowedSteps:", a);
    return a;
  }, [stepStatus]);

  return {
    status,
    stepStatus,

    loading,
    saving,
    error,

    onboardingComplete,
    pipelineStarted,
    pipelineRunning,
    dashboardReady,

    allowedSteps,

    completeStep,
    finish,
    reset,
    refresh: fetchStatus,
  };
}
