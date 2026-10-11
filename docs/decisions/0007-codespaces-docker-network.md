# 0007: Fixing Docker networking in GitHub Codespaces

**Status:** accepted, milestone 1

## Context

The project runs in a GitHub codespace on the default image. In October 2026 that image starts
Docker with firewall rules written through `iptables-nft`. Older `iptables-legacy` rules are
also active, though: their `FORWARD` policy is `DROP`, and they only allow Docker's default
bridge (`docker0`). Linux applies both rule sets, so every packet on a Compose network is
dropped. Containers can't reach each other or the internet, and the symptom is a timeout, not
an error. Health checks still pass, because they run inside each container.

## Decision

`make up` and `make replay-up` first run `make codespace-network`. When `CODESPACES=true`, the
legacy `FORWARD` policy is `DROP`, and the rule isn't there yet, it inserts two legacy rules
that accept traffic in and out of Docker's bridges (`br-+`), matching what the nft rules
already allow. Anywhere else, it does nothing.

## Consequences

- The stack works in a default codespace without a custom dev container.
- The rule doesn't survive a restart of the codespace, which is why it runs on every `make up`.
- If GitHub fixes the image, the condition no longer matches and the step does nothing.
