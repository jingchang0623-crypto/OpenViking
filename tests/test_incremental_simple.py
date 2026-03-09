#!/usr/bin/env python
# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
Basic tests for incremental resource update functionality.
"""

from unittest.mock import Mock, MagicMock
from openviking.resource import (
    ResourceLockManager,
    ResourceLockConflictError,
    DiffDetector,
    DiffResult,
    FileHash,
    StagingManager,
    StagingArea,
    PublicationManager,
    VectorReuseManager,
    IncrementalUpdater,
)


def test_lock_creation():
    """Test lock creation and release."""
    print("Testing lock creation...")
    
    agfs = Mock()
    agfs.exists = Mock(side_effect=lambda path: "/local/.locks" in path and path.endswith(".locks"))
    agfs.mkdir = Mock()
    agfs.write = Mock()
    agfs.rm = Mock()
    
    manager = ResourceLockManager(agfs)
    lock_info = manager.acquire_lock(
        resource_uri="viking://resources/test",
        operation="test_op",
    )
    
    assert lock_info.resource_uri == "viking://resources/test"
    assert lock_info.operation == "test_op"
    assert lock_info.lock_id is not None
    
    agfs.exists = Mock(return_value=True)
    agfs.read = Mock(return_value=f'{{"lock_id": "{lock_info.lock_id}", "resource_uri": "viking://resources/test", "operation": "test_op", "created_at": 0, "expires_at": null, "metadata": {{}}}}'.encode())
    
    result = manager.release_lock("viking://resources/test", lock_info.lock_id)
    assert result is True
    
    print("✓ Lock creation test passed")


def test_lock_conflict():
    """Test lock conflict detection."""
    print("Testing lock conflict...")
    
    agfs = Mock()
    agfs.exists = Mock(return_value=True)
    agfs.mkdir = Mock()
    agfs.read = Mock(return_value=b'{"lock_id": "existing-lock", "resource_uri": "viking://resources/test", "operation": "other_op", "created_at": 0, "expires_at": null, "metadata": {}}')
    
    manager = ResourceLockManager(agfs)
    
    try:
        manager.acquire_lock(
            resource_uri="viking://resources/test",
            operation="test_op",
        )
        assert False, "Should have raised ResourceLockConflictError"
    except ResourceLockConflictError as e:
        assert "viking://resources/test" in str(e)
    
    print("✓ Lock conflict test passed")


def test_file_hash_calculation():
    """Test file hash calculation."""
    print("Testing file hash calculation...")
    
    agfs = Mock()
    agfs.exists = Mock(return_value=True)
    agfs.isdir = Mock(return_value=False)
    
    file_mock = MagicMock()
    file_mock.read = Mock(side_effect=[b"test content", b""])
    file_mock.__enter__ = Mock(return_value=file_mock)
    file_mock.__exit__ = Mock(return_value=False)
    agfs.open = Mock(return_value=file_mock)
    
    detector = DiffDetector(agfs)
    file_hash = detector.calculate_file_hash("/test/file.txt")
    
    assert file_hash is not None
    assert file_hash.path == "/test/file.txt"
    assert file_hash.content_hash is not None
    assert file_hash.size > 0
    assert not file_hash.is_directory
    
    print("✓ File hash calculation test passed")


def test_diff_detection():
    """Test diff detection between versions."""
    print("Testing diff detection...")
    
    old_hashes = {
        "/test/file1.txt": FileHash(
            path="/test/file1.txt",
            content_hash="hash1",
            size=100,
            is_directory=False,
        ),
        "/test/file2.txt": FileHash(
            path="/test/file2.txt",
            content_hash="hash2",
            size=200,
            is_directory=False,
        ),
    }
    
    new_hashes = {
        "/test/file1.txt": FileHash(
            path="/test/file1.txt",
            content_hash="hash1",
            size=100,
            is_directory=False,
        ),
        "/test/file2.txt": FileHash(
            path="/test/file2.txt",
            content_hash="hash2_modified",
            size=250,
            is_directory=False,
        ),
        "/test/file3.txt": FileHash(
            path="/test/file3.txt",
            content_hash="hash3",
            size=150,
            is_directory=False,
        ),
    }
    
    detector = DiffDetector(Mock())
    diff_result = detector.detect_diff(old_hashes, new_hashes)
    
    assert len(diff_result.unchanged_files) == 1
    assert len(diff_result.modified_files) == 1
    assert len(diff_result.added_files) == 1
    assert len(diff_result.deleted_files) == 0
    assert diff_result.has_changes()
    
    stats = diff_result.get_stats()
    assert stats["added_files"] == 1
    assert stats["modified_files"] == 1
    assert stats["unchanged_files"] == 1
    
    print("✓ Diff detection test passed")


def test_staging_area_creation():
    """Test staging area creation."""
    print("Testing staging area creation...")
    
    agfs = Mock()
    agfs.exists = Mock(return_value=False)
    agfs.mkdir = Mock()
    
    manager = StagingManager(agfs)
    staging_area = manager.create_staging_area("viking://resources/test")
    
    assert staging_area.target_uri == "viking://resources/test"
    assert staging_area.staging_id is not None
    assert ".staging" in staging_area.staging_path
    
    print("✓ Staging area creation test passed")


async def test_filesystem_switch():
    """Test filesystem switch."""
    print("Testing filesystem switch...")
    
    agfs = Mock()
    agfs.exists = Mock(return_value=True)
    agfs.rm = Mock()
    agfs.mkdir = Mock()
    agfs.mv = Mock()
    
    vector_backend = Mock()
    
    manager = PublicationManager(agfs, vector_backend)
    
    staging_area = StagingArea(
        staging_uri="viking://default/.staging/test-staging",
        staging_path="/.staging/test-staging",
        target_uri="viking://default/resources/test",
        target_path="/resources/test",
        staging_id="test-staging",
    )
    
    result = await manager.switch_filesystem(staging_area)
    assert result is True
    
    print("✓ Filesystem switch test passed")


def main():
    """Run all tests."""
    print("=" * 60)
    print("Running incremental update tests...")
    print("=" * 60)
    
    test_lock_creation()
    test_lock_conflict()
    test_file_hash_calculation()
    test_diff_detection()
    test_staging_area_creation()
    
    import asyncio
    asyncio.run(test_filesystem_switch())
    
    print("=" * 60)
    print("All tests passed! ✓")
    print("=" * 60)


if __name__ == "__main__":
    main()
