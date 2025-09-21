import hashlib
import os
import yaml


# Function for generating file hash values
def get_file_hash(file_path: str, algorithm: str = "sha256"):
    try:
        hash_func = hashlib.new(algorithm)

        with open(file_path, "rb") as file:
            while chunk := file.read(8192):
                hash_func.update(chunk)

        return hash_func.hexdigest()
    except FileNotFoundError:
        print("[ERR] File not found! [local::get_file_hash]")
        return None
    except ValueError:
        print("[ERR] Invalid parameter given! [local::get_file_hash]")
        return None
    except Exception as e:
        print(f"[ERR] Exception occured! [local::get_file_hash]\n{e}")
        return None


