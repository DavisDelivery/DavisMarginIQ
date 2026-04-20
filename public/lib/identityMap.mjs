// MarginIQ Canonical Identity Mapping
// Resolves raw payroll names (W2 employees and 1099 entities) to a stable
// canonical person identity, and optionally to a truck assignment.
//
// Three layers:
//   1. entityName  — exact string from the payroll PDF (e.g. "BLESSED STAR LLC")
//   2. personKey   — canonical person identifier (e.g. "person_blessed_star_owner")
//   3. truckNumber — optional truck assignment (e.g. "0608")
//
// Storage (Firebase):
//   collection: identityMap
//     docs keyed by canonicalKey (from parsePayroll.mjs)
//       { rawNames: [string], personKey, personDisplayName,
//         kind: 'W2'|'1099', truckNumber?, notes?,
//         createdAt, updatedAt }
//   collection: persons
//     docs keyed by personKey
//       { displayName, role?, phone?, startDate?, truckNumbers: [string] }

import { canonicalKey } from './parsePayroll.mjs';

// ============================================================================
// SEED MAPPINGS — starting point, user corrects/extends via UI
// ============================================================================

// Self-documenting: "Jean Delsoin dba: CDM Transportation" parses as
//   person = "Jean Delsoin"
//   entity = "CDM Transportation"
export function extractDbaName(rawName) {
  const m = String(rawName).match(/^(.+?)\s+dba:\s+(.+)$/i);
  if (!m) return null;
  return { person: m[1].trim(), entity: m[2].trim() };
}

// Seeds based on what Chad has confirmed from prior sessions (userMemories).
// Truck numbers from SENTINEL mapping are partial — rest filled in by Chad via UI.
export const SEED_IDENTITY_MAP = [
  // ---------- W2 drivers with known truck assignments ----------
  { canonicalKey: 'TREVOR_SYERS',      personDisplayName: 'Trevor Syers',        kind: 'W2',   truckNumber: '0608', rawNames: ['TREVOR SYERS'] },
  { canonicalKey: 'TREVARR_HOWARD',    personDisplayName: 'Trevarr Howard',      kind: 'W2',   truckNumber: '4757', rawNames: ['TREVARR HOWARD'] },
  { canonicalKey: 'BRENT_DIXON',       personDisplayName: 'Brent Dixon',         kind: 'W2',   truckNumber: '0294', rawNames: ['BRENT DIXON'] },

  // ---------- 1099 contractors where person name is the entity ----------
  { canonicalKey: 'ALFRED_E_ANDI',           personDisplayName: 'Alfred E. Andi',        kind: '1099', rawNames: ['ALFRED E ANDI'] },
  { canonicalKey: 'COLIN_CALHOUN',           personDisplayName: 'Colin Calhoun',         kind: '1099', rawNames: ['COLIN CALHOUN'] },
  { canonicalKey: 'DON_JUAN_MCCRARY',        personDisplayName: 'Don Juan McCrary',      kind: '1099', rawNames: ['DON JUAN McCRARY'] },
  { canonicalKey: 'GEORGE_THOMAS_LEONARD',   personDisplayName: 'George Thomas Leonard', kind: '1099', rawNames: ['GEORGE THOMAS LEONARD'] },
  { canonicalKey: 'JAMES_A_BENNETT',         personDisplayName: 'James A. Bennett',      kind: '1099', rawNames: ['JAMES A BENNETT'] },

  // ---------- 1099 contractor LLC with dba pattern (auto-extracted) ----------
  { canonicalKey: 'JEAN_DELSOIN_DBA_CDM_TRANSPORTATION',
    personDisplayName: 'Jean Delsoin',
    entityName: 'CDM Transportation',
    kind: '1099',
    rawNames: ['Jean Delsoin dba: CDM Transportation'] },

  // ---------- 1099 contractor LLCs awaiting Chad's mapping (person unknown) ----------
  { canonicalKey: 'BLESSED_STAR_LLC',                   entityName: 'Blessed Star LLC',                   kind: '1099', rawNames: ['BLESSED STAR LLC'],                   needsMapping: true },
  { canonicalKey: 'FRANK_EXPRESS_DELIVERY',             entityName: 'Frank Express Delivery',             kind: '1099', rawNames: ['FRANK EXPRESS DELIVERY'],             needsMapping: true },
  { canonicalKey: 'IMPACT_GLOBAL_NETWORK_LLC',          entityName: 'Impact Global Network LLC',          kind: '1099', rawNames: ['IMPACT GLOBAL NETWORK LLC'],          needsMapping: true },
  { canonicalKey: 'KENS_FREIGHT_LLC',                   entityName: "Ken's Freight LLC",                  kind: '1099', rawNames: ['KENS FREIGHT LLC'],                   needsMapping: true },
  { canonicalKey: 'LION_OF_JUDAH_EXPRESS_LLC',          entityName: 'Lion of Judah Express LLC',          kind: '1099', rawNames: ['LION OF JUDAH EXPRESS LLC'],          needsMapping: true },
  { canonicalKey: 'MASKAN_LOGISTICS_LLC',               entityName: 'Maskan Logistics LLC',               kind: '1099', rawNames: ['MASKAN LOGISTICS LLC'],               needsMapping: true },
  { canonicalKey: 'MGH_INVESTORS',                      entityName: 'MGH Investors',                      kind: '1099', rawNames: ['MGH INVESTORS'],                      needsMapping: true },
  { canonicalKey: 'MIDAS_LLC',                          entityName: 'Midas LLC',                          kind: '1099', rawNames: ['MIDAS LLC'],                          needsMapping: true },
  { canonicalKey: 'NELSON_DELIVERY_LOGISTICS_LLC',     entityName: 'Nelson Delivery & Logistics LLC',    kind: '1099', rawNames: ['NELSON DELIVERY & LOGISTICS LLC'],    needsMapping: true },
  { canonicalKey: 'RDC_TRANSPORT_LLC',                  entityName: 'RDC Transport LLC',                  kind: '1099', rawNames: ['RDC TRANSPORT LLC'],                  needsMapping: true },
  { canonicalKey: 'ROSA_DELIVERY_SERVICE',              entityName: 'Rosa Delivery Service',              kind: '1099', rawNames: ['ROSA DELIVERY SERVICE'],              needsMapping: true },
  { canonicalKey: 'SAVE_HAVEN_LOGISTICS_LLC',           entityName: 'Save Haven Logistics LLC',           kind: '1099', rawNames: ['SAVE HAVEN LOGISTICS LLC'],           needsMapping: true },
  { canonicalKey: 'VINCENT_DELIVERY_SERVICE',           entityName: 'Vincent Delivery Service',           kind: '1099', rawNames: ['VINCENT DELIVERY SERVICE'],           needsMapping: true },
  { canonicalKey: 'WYATT_AND_SONS_TRANSPORTATION_LLC',  entityName: 'Wyatt and Sons Transportation LLC', kind: '1099', rawNames: ['WYATT AND SONS TRANSPORTATION LLC'],  needsMapping: true },
];

// ============================================================================
// LOOKUP / APPLY
// ============================================================================

// Given a list of parsed payroll employees (from parsePayroll.mjs) and a map
// keyed by canonicalKey, return enriched records with person+truck info.
export function enrichEmployees(parsedEmployees, identityMap) {
  const mapByKey = new Map(identityMap.map(m => [m.canonicalKey, m]));
  return parsedEmployees.map(e => {
    const match = mapByKey.get(e.canonicalKey);
    if (!match) {
      // Unmapped — the UI should surface this for Chad to assign.
      return {
        ...e,
        identity: {
          status: 'unmapped',
          displayName: e.rawName,
          needsMapping: true,
          kind: null,
          truckNumber: null,
        },
      };
    }
    return {
      ...e,
      identity: {
        status: match.needsMapping ? 'needs_person' : 'ok',
        displayName: match.personDisplayName || match.entityName || e.rawName,
        personDisplayName: match.personDisplayName || null,
        entityName: match.entityName || null,
        kind: match.kind,
        truckNumber: match.truckNumber || null,
        needsMapping: !!match.needsMapping || !match.truckNumber,
      },
    };
  });
}

// Quick summary: how many mapped vs unmapped, how many with truck assignments
export function mappingHealth(enriched) {
  const h = {
    total: enriched.length,
    fullyMapped: 0,
    needsPersonName: 0,
    needsTruck: 0,
    unknown: 0,
  };
  for (const e of enriched) {
    const i = e.identity;
    if (i.status === 'unmapped') h.unknown++;
    else if (i.status === 'needs_person') h.needsPersonName++;
    else if (!i.truckNumber) h.needsTruck++;
    else h.fullyMapped++;
  }
  return h;
}
