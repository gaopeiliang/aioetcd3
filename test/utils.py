async def switch_auth_on(client):
    await client.user_add(username="root", password="root")
    await client.role_add(name="root")
    await client.user_grant_role(username="root", role="root")

    await client.user_add(username="client", password="client")
    await client.role_add(name="client")
    await client.user_grant_role(username="client", role="client")
    await client.auth_enable()


async def switch_auth_off(root_client, unautheticated_client):
    await root_client.auth_disable()
    await unautheticated_client.user_delete("client")
    await unautheticated_client.user_delete("root")
    await unautheticated_client.role_delete("client")
    await unautheticated_client.role_delete("root")
