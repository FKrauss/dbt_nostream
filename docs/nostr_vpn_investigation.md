# Nostr VPN Investigation Summary

**Investigated:** npub1xdhnr9mrv47kkrn95k6cwecearydeh8e895990n3acntwvmgk2dsdeeycm (Sirius Business Ltd)
**Repo:** https://git.iris.to/nostr-vpn
**Status:** No public protocol events found. Traffic is private.

## Findings (from BQ relay data)

| Observation | Detail |
|---|---|
| Developer public events | Only kind 0 (profile) and kind 3 (contacts). Zero protocol events. |
| Contact count | Exactly 1 unique peer in the developer's contact list. |
| Encrypted DM (kind 4) volume | 0 in our relay slice. |
| Custom event kinds | None observed from this npub. |
| Relay URLs with VPN pattern | None in relay lists (kind 10002, 3). |

## What this means

Public relay data captures only social signals (people *talking about* VPNs, NostrVPN, nostrvpn.org links). The actual VPN invitation, network setup, or peer-discovery protocol operates out of band — likely via encrypted DMs, private/permissioned relays, or entirely off-relay.

**Conclusion for our dataset:** Nostr VPN network/server counts are not measurable from the public event stream.

## Social signal proxy (30 days)

- Text notes mentioning VPN: thousands (kind 1)
- Reposts mentioning VPN: hundreds (kind 6)
- Zaps in this time window: 25 (mostly unrelated bolt11 noise)

That's all.
