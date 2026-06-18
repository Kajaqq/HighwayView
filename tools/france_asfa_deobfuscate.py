import re

import aiohttp
import winloop

from config import CONSTANTS
from Downloaders.base_downloader import GenericDownloader

BASE_URL = CONSTANTS.FRANCE.ASFA.BASE_URL
AUTH_URL = CONSTANTS.FRANCE.ASFA.AUTH_URL
HTTPS_PREFIX = CONSTANTS.COMMON.HTTPS_PREFIX
CAMERA_SUFFIX = CONSTANTS.FRANCE.ASFA.CAMERA_SUFFIX


async def get_auth_key(session, url) -> str | None:
    """
    Fetches the authorization key from the initial ASFA authentication endpoint.

    Args:
        session (aiohttp.ClientSession): The active client session.
        url (str): The starting URL.

    Returns:
        str | None: The extracted authentication key or None if extraction fails.
    """
    key = None
    async with session.get(url) as r:
        r.raise_for_status()
        async for line in r.content:
            line_text = line.decode("utf-8")
            try:
                if line_text.startswith("WT3_AuthenticateWebSite"):
                    parts = line_text.split("'")
                    key = parts[3]
                    break
            except (IndexError, Exception) as e:
                print(f"Error parsing auth key: {e}")
    return key


async def get_phase2(session, key, downloader: GenericDownloader, url=AUTH_URL) -> str:
    """
    Constructs the Phase 2 URL via the authentication key.

    Args:
        session (aiohttp.ClientSession): The active client session.
        key (str): The authentication key.
        downloader (GenericDownloader): The generic downloader utility.
        url (str, optional): The phase 1 auth URL format string. Defaults to AUTH_URL.

    Returns:
        str: The URL for phase 2.
    """
    url = url.format(key=key)
    content: str = await downloader.download(url=url, session=session)
    phase2_parts = content.split(";")
    phase2_valid = [
        x for x in phase2_parts if x.startswith("WT3_SawtLinkToPhase2.src =")
    ]
    phase2_url: str = phase2_valid[0].split("'")[1]
    phase2_url = HTTPS_PREFIX + phase2_url
    return phase2_url


def resolve_js_variables(lines: list[str], target_domain="www.autoroutes.fr") -> dict[str, str]:
    """
    Resolves obfuscated variables mimicking a JavaScript execution flow.

    Args:
        lines (list[str]): The lines of JavaScript to evaluate.
        target_domain (str, optional): The target domain condition to simulate. Defaults to "www.autoroutes.fr".

    Returns:
        dict[str, str]: A dictionary of resolved JavaScript variable values.
    """
    var_map: dict[str, str] = {}

    # regex to match: var NAME = 'VALUE' or "VALUE"
    var_pattern = re.compile(
        r"var\s+(\w+)\s*=\s*['\"]([^'\"]*)['\"](?:\.substring\((\d+),(\d+)\))?"
    )

    for line in lines:
        line = line.strip()

        # 1. Handle Domain Conditionals: if(document.domain == '...') { var X = 'Y' }
        # We look for the assignment that matches our domain OR the 'else' block
        if "document.domain" in line:
            is_match = f"'{target_domain}'" in line
            is_negation = "!=" in line

            # Logic: (== and match) OR (!= and no match) -> Take the 'if' value
            # Otherwise, we'd wait for the 'else' block (which usually follows in the list)
            should_take = (is_match and not is_negation) or (
                is_negation and not is_match
            )

            match = var_pattern.search(line)
            if match and should_take:
                name, val, start, end = match.groups()
                var_map[name] = val[int(start) : int(end)] if start else val
            continue

        # 2. Handle standard assignments (including those in 'else' blocks that didn't match above)
        match = var_pattern.search(line)
        if match:
            name, val, start, end = match.groups()
            # Only update if not already set by a specific domain condition
            if name not in var_map:
                var_map[name] = val[int(start) : int(end)] if start else val

    return var_map


def assemble_url(phase2_vars: list[str], var_values: dict[str, str]) -> str:
    """
    Assembles the final dataset URL using the resolved components.

    Args:
        phase2_vars (list[str]): The phase 2 js instructions.
        var_values (dict[str, str]): The resolved variables from `resolve_js_variables`.

    Raises:
        ValueError: If the required variable concatenations cannot be found.

    Returns:
        str: The fully assembled URL.
    """
    descriptor_line = next(
        (x for x in phase2_vars if "var SAWT3_WebcamDescriptorsLocation =" in x), None
    )

    if descriptor_line:
        # Extract the sum components: var X = A + B + C
        parts_string = descriptor_line.split("=")[1].strip().rstrip(";")
        var_names = [v.strip() for v in parts_string.split("+")]

        # Join the resolved values
        final_path = "".join(var_values.get(name, "") for name in var_names)
        full_url = HTTPS_PREFIX + final_path + CAMERA_SUFFIX
    else:
        raise ValueError("Descriptor location not found in phase2 script.")
    return full_url


async def get_complete_url() -> str:
    """
    Orchestrates the complete process to deobfuscate and retrieve the full ASFA data URL.

    Returns:
        str: Expected data URL.
    """
    downloader = GenericDownloader()
    headers, timeout, connector = await downloader.get_settings()
    async with aiohttp.ClientSession(
        headers=headers, connector=connector, timeout=timeout
    ) as session:
        auth_key = await get_auth_key(session, BASE_URL)
        if not auth_key:
            raise ValueError("Failed to get auth key.")
        phase_2_url = await get_phase2(session, auth_key, downloader)
        phase_2_list = await downloader.download(url=phase_2_url, session=session)
        phase_2_vars = phase_2_list.split(";")
        resolved_vars = resolve_js_variables(phase_2_vars)
        full_url = assemble_url(phase_2_vars, resolved_vars)
    return full_url


if __name__ == "__main__":
    asfa_data = winloop.run(get_complete_url())
    print(f"ASFA data URL: {asfa_data}")
