from pathlib import Path
import tableauserverclient as TSC


def publish_to_tableau(
    hyper_path: Path,
    slack_message: str,
    server_url: str,
    site_id: str,
    pat_name: str,
    pat_secret: str,
    project_name: str = "Default",
    datasource_name: str = "bank_ads_latest",
    slack_webhook: str = "",
    notify_slack_fn=None,
):
    missing = [k for k, v in {
        "TABLEAU_SERVER_URL": server_url,
        "TABLEAU_SITE_ID": site_id,
        "TABLEAU_PAT_NAME": pat_name,
        "TABLEAU_PAT_SECRET": pat_secret,
    }.items() if not v]
    if missing:
        raise SystemExit(f"Missing Tableau env vars: {', '.join(missing)}")

    server = TSC.Server(server_url, use_server_version=True)
    auth = TSC.PersonalAccessTokenAuth(pat_name, pat_secret, site_id=site_id)

    with server.auth.sign_in(auth):
        project_item = None
        for p in TSC.Pager(server.projects):
            if p.name == project_name:
                project_item = p
                break
        if not project_item:
            raise RuntimeError(f"Project not found: {project_name!r}")

        ds_item = TSC.DatasourceItem(project_item.id, datasource_name)

        print(f"[TABLEAU] Publishing datasource '{datasource_name}' to project '{project_name}' ...")
        server.datasources.publish(ds_item, str(hyper_path), mode=TSC.Server.PublishMode.Overwrite)
        print("[TABLEAU] Publish OK")

    if notify_slack_fn is not None:
        notify_slack_fn(slack_webhook, slack_message)
