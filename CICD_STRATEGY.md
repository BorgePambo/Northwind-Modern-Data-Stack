# Estratégia CI/CD - dbt + Databricks + Airflow

## 📋 Visão Geral

Este documento descreve a estratégia profissional de CI/CD para o projeto dbt com Databricks, utilizando GitHub Actions com dois ambientes: **DEV** e **PROD**.

---

## 🔄 Fluxo esperado

```
feature branch
    ↓
    └──→ Pull Request → develop
         ├─ Executa: ci.yml (validação)
         ├─ dbt deps, parse, build (dry-run)
         └─ Merge para develop
              ↓
              └──→ deploy-dev.yml (automático)
                   ├─ dbt build (com dados reais)
                   ├─ dbt test
                   ├─ Deploy em dbt_northwind_dev
                   └─ Sucesso ✅ (ou falha ❌)

Após validação em DEV:

develop
    ↓
    └──→ Pull Request → main
         ├─ Executa: ci.yml (validação)
         ├─ dbt deps, parse, build (dry-run)
         └─ Merge para main
              ↓
              └──→ deploy-prod.yml (automático)
                   ├─ dbt build (com dados reais)
                   ├─ dbt test
                   ├─ Deploy em dbt_northwind_prod
                   └─ Sucesso ✅ (ou falha ❌)
```

---

## 📁 Estrutura dos Workflows

### 1. `ci.yml` - Validação contínua

**Quando executa:**
- Em Pull Requests para `develop` e `main`
- Manualmente via `workflow_dispatch`

**O que faz:**
- ✅ Instala dbt-core 1.12.0 e dbt-databricks 1.10.0
- ✅ Executa `dbt debug` (valida conexão)
- ✅ Executa `dbt deps` (baixa packages)
- ✅ Executa `dbt parse` (valida sintaxe)
- ✅ Executa `dbt build --select state:modified+` (valida modelos modificados)
- ✅ Executa testes Python (pytest)
- ✅ Faz upload de artefatos

**Não faz deploy** - apenas valida o código!

---

### 2. `deploy-dev.yml` - Deploy DEV

**Quando executa:**
- Automaticamente após push/merge na branch `develop`
- Manualmente via `workflow_dispatch`

**O que faz:**
- ✅ Usa GitHub Environment `dev` (secrets isolados)
- ✅ Cria `profiles.yml` com credenciais DEV do Databricks
- ✅ Executa `dbt build` **com dados reais** em `dbt_northwind_dev`
- ✅ Executa `dbt test` (valida dados)
- ✅ Gera documentação (`dbt docs generate`)
- ✅ Faz upload de artefatos por 7 dias

**Segurança:**
- Credenciais vêm de `environment: dev` (isolado)
- Schema específico: `dbt_northwind_dev`
- 4 threads (mais leve que PROD)

---

### 3. `deploy-prod.yml` - Deploy PROD

**Quando executa:**
- Automaticamente após push/merge na branch `main`
- Manualmente via `workflow_dispatch`

**O que faz:**
- ✅ Usa GitHub Environment `prod` (secrets isolados)
- ✅ Cria `profiles.yml` com credenciais PROD do Databricks
- ✅ Executa `dbt build` **com dados reais** em `dbt_northwind_prod`
- ✅ Executa `dbt test` (validação obrigatória - não permite skip)
- ✅ Gera documentação
- ✅ Faz upload de artefatos por 30 dias

**Segurança:**
- Credenciais vêm de `environment: prod` (isolado)
- Schema específico: `dbt_northwind_prod`
- 8 threads (melhor performance)
- Testes são obrigatórios (sem `continue-on-error`)

---

## 🔐 Configuração de Secrets

### Para o Environment `dev`:

Na aba **Settings** → **Environments** → **dev**, adicione:

```
DATABRICKS_HOST         = <seu-workspace-url>
DATABRICKS_TOKEN        = <seu-token-dev>
DATABRICKS_HTTP_PATH    = /sql/1.0/warehouses/<warehouse-id-dev>
DATABRICKS_CATALOG      = dbt_northwind (mesmo catalog, schema diferente)
```

### Para o Environment `prod`:

Na aba **Settings** → **Environments** → **prod**, adicione:

```
DATABRICKS_HOST         = <seu-workspace-url>
DATABRICKS_TOKEN        = <seu-token-prod>
DATABRICKS_HTTP_PATH    = /sql/1.0/warehouses/<warehouse-id-prod>
DATABRICKS_CATALOG      = dbt_northwind (mesmo catalog, schema diferente)
```

---

## ⚙️ Versões Fixadas

As versões foram escolhidas por compatibilidade testada:

```yaml
Python:         3.12
dbt-core:       1.12.0
dbt-databricks: 1.10.0
```

### Por que não `latest`?

- **Reprodutibilidade**: Garante que CI sempre usa as mesmas versões
- **Compatibilidade**: Evita quebras entre versões incompatíveis
- **Predictabilidade**: Reduz bugs aleatórios

### Compatibilidade verificada:

- `dbt-core 1.12.0` → Suporta `dbt-databricks 1.10.0` ✅
- `dbt-databricks 1.10.0` → Suporta `Python 3.12` ✅
- Todas as dependências em `requirements.txt` são compatíveis ✅

---

## 🛡️ Segurança - Separação DEV/PROD

### Garantias implementadas:

1. **GitHub Environments**
   - `dev` e `prod` têm secrets separados
   - DEV nunca consegue acessar tokens PROD
   - PROD nunca consegue acessar credenciais DEV

2. **Schemas diferentes**
   - DEV: `dbt_northwind_dev`
   - PROD: `dbt_northwind_prod`
   - Evita sobrescrita acidental

3. **Sem hardcoding**
   - Todos os secrets vêm de `${{ secrets.* }}`
   - `profiles.yml` é gerado dinamicamente
   - Nenhuma credencial no git

4. **Testes obrigatórios em PROD**
   - `deploy-dev.yml`: `continue-on-error: true` (permite passar com testes falhando)
   - `deploy-prod.yml`: `continue-on-error: false` (falha se algum teste falhar)

---

## 🚀 Como usar

### 1. Criar feature branch

```bash
git checkout -b feature/meu-modelo
# ... fazer alterações ...
git add .
git commit -m "Add novo modelo"
git push -u origin feature/meu-modelo
```

### 2. Abrir Pull Request para `develop`

```
GitHub → New Pull Request
Base: develop ← Compare: feature/meu-modelo
```

- ✅ `ci.yml` executa automaticamente
- ✅ Valida o código
- ✅ Se passar: Merge

### 3. Deploy automático em DEV

Após merge em `develop`:
- ✅ `deploy-dev.yml` executa automaticamente
- ✅ Deploy realizado em `dbt_northwind_dev`
- ✅ Artefatos salvos por 7 dias

### 4. Merge para `main`

```bash
git checkout main
git pull origin main
git merge develop
git push origin main
```

Ou via GitHub:
- Pull Request: `develop` → `main`
- ✅ `ci.yml` executa
- ✅ Se passar: Merge

### 5. Deploy automático em PROD

Após merge em `main`:
- ✅ `deploy-prod.yml` executa automaticamente
- ✅ Deploy realizado em `dbt_northwind_prod`
- ✅ Artefatos salvos por 30 dias

---

## 📊 Matriz de execução

| Evento | Branch | Workflow | Ação |
|--------|--------|----------|------|
| PR criado | develop | `ci.yml` | Valida |
| PR criado | main | `ci.yml` | Valida |
| Merge em develop | develop | `deploy-dev.yml` | Deploy DEV |
| Merge em main | main | `deploy-prod.yml` | Deploy PROD |
| Qualquer hora | Qualquer | `*` dispatch | Executa manualmente |

---

## 🔍 Troubleshooting

### ❌ Deploy falha com erro de autenticação

**Causa**: Secrets não configurados ou inválidos

**Solução**:
1. Ir em Settings → Environments → `dev` (ou `prod`)
2. Verificar se `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, etc. estão preenchidos
3. Testar credenciais localmente: `dbt debug`

### ❌ dbt parse falha com erro de sintaxe

**Causa**: Arquivo YAML mal formatado

**Solução**:
1. Rodar localmente: `cd airflow_dbt/include/northwind_dbt && dbt parse`
2. Verificar logs de erro
3. Fixar e fazer push

### ❌ Testes falhando em DEV mas não em CI

**Causa**: Dados diferentes entre ambientes

**Solução**:
1. Verificar se o warehouse DEV tem dados
2. Executar `dbt seed` em DEV
3. Revisar asserts nos testes

---

## 💡 Boas práticas

1. **Sempre faça PR para `develop` primeiro**
   - Não fazer merge direto em `main`
   - Permite validação extra

2. **Use `workflow_dispatch` para reruns**
   - Não faça push vazio apenas para retrigger
   - Use Actions → Workflow → Run workflow

3. **Monitore os logs**
   - Actions → Workflow name → Run
   - Verifique `dbt build` e `dbt test` output

4. **Limpe artefatos regularmente**
   - DEV: 7 dias (mais descartável)
   - PROD: 30 dias (mais importante)

5. **Documente mudanças grandes**
   - Adicione descrição na PR
   - Mencione modelos/testes alterados

---

## 📚 Referências

- [GitHub Actions Environments](https://docs.github.com/en/actions/deployment/targeting-different-environments/using-environments-for-deployment)
- [dbt documentation](https://docs.getdbt.com/)
- [Databricks dbt adapter](https://github.com/databricks/dbt-databricks)
- [GitHub Actions best practices](https://docs.github.com/en/actions/guides)

---

**Criado em**: 2026-08-29  
**Versão**: 1.0.0  
**Status**: ✅ Pronto para usar
